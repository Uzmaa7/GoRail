import { StatusCodes } from "http-status-codes";
import AppError from "../utils/errors/appError.js";
import logger from "../config/logger.js";
import { config } from "../config/index.js";
import { inventoryClient } from "./inventoryClient.js";
import { paymentClient } from "./paymentClient.js";
import  saga  from "../services/saga.service.js";
import { acquireSeatLocks, releaseSeatLocks, forceReleaseSeatLocks } from "../utils/distributedLock.js";
import idempotencyRepository from "../repositories/idempotencyRepository.js";
import prisma from "../config/prisma.js";
import bookingProducer from "../kafka/producer/booking.producer.js";


// ─── Create Booking ──────────────────────────────────────────────────────────

export class BookingService {
    constructor(bookingRepository, idempotencyRepository) {
        this.bookingRepository = bookingRepository;
        this.idempotencyRepository = idempotencyRepository;
    }

    // ─── Idempotency Helper ──────────────────────────────────────────────────────

    async checkIdempotency(key) {
        const existing = await this.idempotencyRepository.findByKey(key);
        if (existing) {
            logger.info(`Idempotent request: ${key}`);
            return existing.response;
        }
        return null;
    };

    async saveIdempotency(key, response) {
        await this.idempotencyRepository.saveKey(key, response);
    }

    async createBooking(userId, scheduleId, seatIds, passengers, idempotencyKey,
        fromStationId, toStationId, fromSeq, toSeq) {

        // 1. Validate input
        if (!scheduleId || !seatIds || !Array.isArray(seatIds) || seatIds.length === 0) {
            throw new AppError("scheduleId and seatIds (non empty array) are required", StatusCodes.BAD_REQUEST);
        }

        if (!passengers || !Array.isArray(passengers) || passengers.length === 0) {
            throw new AppError("passengers(non empty array) is required", StatusCodes.BAD_REQUEST);
        }

        if (seatIds.length !== passengers.length) {
            throw new AppError("Number of seats must be match with number of passengers", StatusCodes.BAD_REQUEST);
        }

        if (!idempotencyKey) {
            throw new AppError("idempotency is required", StatusCodes.BAD_REQUEST);
        }

        // --- SEGMENT BOOKING: Validate segment params if provided ---
        if (fromSeq && toSeq && fromSeq >= toSeq) {
            throw new AppError("fromStation must come before toStation", StatusCodes.BAD_REQUEST);
        }

        // ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════
        // 2. Check idempotency
        const cached = await this.checkIdempotency(`booking:${idempotencyKey}`);
        if (cached) return cached;


        // ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════
        // 3. Fetch schedule availability and seat details from inventory
        const availability = await inventoryClient.getAvailability(scheduleId);
        if (availability.status !== "ACTIVE") {
            throw new AppError("Schedule is not ACTIVE", StatusCodes.BAD_REQUEST);
        }

        // Prevent booking trains that have already departed
        if (new Date(availability.departureDate) < new Date()) {
            throw new AppError("Cannot book a train that has already departed", StatusCodes.BAD_REQUEST);
        }

        // --- SEGMENT BOOKING: Pass segment params to get segment-aware seat availability ---
        const seatData = await inventoryClient.getSeats(scheduleId,
            {
                fromSeq: fromSeq || undefined,
                toSeq: toSeq || undefined,
            }
        )

        const seatMap = new Map(seatData.seats.map((s) => [s.seatId, s]));

        // Verify all requested seats - exist and are available
        const bookingSeats = [];
        let totalAmount = 0;

        for (const seatId of seatIds) {
            const seat = seatMap.get(seatId);
            if (!seat) {
                throw new AppError(`Seat ${seatId} not found in schedule`, StatusCodes.NOT_FOUND);
            }

            // --- SEGMENT BOOKING: Use segmentStatus when available for segment-aware validation ---
            const isAvailable = (fromSeq && toSeq && seat.segmentStatus !== undefined)
                ? seat.segmentStatus === 'AVAILABLE'
                : seat.status === 'AVAILABLE';

            if (!isAvailable) {
                throw new AppError(`Seat #${seat.seatNumber} is not available for this segment, SEATS_UNAVAILABLE`, StatusCodes.CONFLICT);
            }

            bookingSeats.push(seat);
            totalAmount += seat.price;
        }


        // ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════
        // 4. Sort seatIds (deadlock prevention for distributed locks)
        // const sortedSeatIds = seatIds.sort(); // ❌ Original 'seatIds' array would change
        const sortedSeatIds = [...seatIds].sort();

        // 5. Acquire Redis distributed locks (segment-aware keys for segment bookings)
        const { acquired, lockValue } = await acquireSeatLocks(
            scheduleId,
            sortedSeatIds,
            `pre-${Date.now()}`, // temporary ID before booking is created
            config.BOOKING_TTL_SECONDS,
            fromSeq,  // --- SEGMENT BOOKING: include in lock key
            toSeq     // --- SEGMENT BOOKING: include in lock key
        );

        if (!acquired) {
            throw new AppError('One or more seats are being booked by another user. Please try again. SEATS_LOCKED', StatusCodes.CONFLICT);
        }


        // ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════
        let booking;

        try {

            // 6. Create booking record in DB
            const lockExpiresAt = new Date(Date.now() + config.BOOKING_TTL_SECONDS * 1000);

            const bookingData = {
                userId,
                scheduleId,
                trainId: availability.trainId,
                trainNumber: availability.trainNumber,
                trainName: availability.trainName,
                departureDate: new Date(availability.departureDate),
                status: 'PENDING',
                totalAmount,
                seatCount: seatIds.length,
                fromStationId: fromStationId || null,  // --- SEGMENT BOOKING
                toStationId: toStationId || null,      // --- SEGMENT BOOKING
                fromSeq: fromSeq || null,              // --- SEGMENT BOOKING
                toSeq: toSeq || null,                  // --- SEGMENT BOOKING
                idempotencyKey,
                lockExpiresAt,
                seats: {
                    create: bookingSeats.map((seat, index) => ({
                        seatId: seat.seatId,
                        seatNumber: seat.seatNumber,
                        seatType: seat.seatType,
                        price: seat.price,
                    })),
                },
                passengers: {
                    create: passengers.map((p, index) => ({
                        name: p.name,
                        age: p.age,
                        gender: p.gender,
                        seatId: seatIds[index] || null, // use original order to match user's intended seat assignment
                    })),
                },
            }

            const includeOptions = {
                seats: true,
                passengers: true,
            };

            booking = await this.bookingRepository.createBookingWithDetails(bookingData, includeOptions);


            // ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════// ═══════════════════════════════════════════════════
            // 7. Execute saga Step 1: Hold seats in inventory
            await saga.executeHoldSeats(booking, sortedSeatIds, config.LOCK_TTL_SECONDS, fromSeq, toSeq); // --- SEGMENT BOOKING

            // 8. Execute saga Step 2: Create payment order
            const paymentOrder = await saga.executeCreatePayment(booking);

            // Refresh booking after updates
            booking = await this.bookingRepository.findBookingWithDetails(booking.id,
                {
                    seats: true,
                    passengers: true
                }
            );

            // 9. Save idempotency
            const response = {
                bookingId: booking.id,
                status: booking.status,
                totalAmount: booking.totalAmount,
                lockExpiresAt: booking.lockExpiresAt,
                seats: booking.seats.map(s => ({
                    seatId: s.seatId,
                    seatNumber: s.seatNumber,
                    seatType: s.seatType,
                    price: s.price,
                })),
                passengers: booking.passengers.map(p => ({
                    name: p.name,
                    age: p.age,
                    gender: p.gender,
                })),
                paymentOrder: {
                    paymentOrderId: paymentOrder.paymentOrderId,
                    gatewayOrderId: paymentOrder.gatewayOrderId,
                    amount: paymentOrder.amount,
                    currency: paymentOrder.currency,
                    keyId: paymentOrder.keyId,
                },
            };

            await this.saveIdempotency(`booking:${idempotencyKey}`, response);

            return response;

        } catch (error) {
            // Compensate on failure
            logger.error(`Booking creation failed for user ${userId}`, { error: error.message });

            if (booking) {
                await saga.compensateAll(booking, sortedSeatIds);
                await prisma.booking.update({
                    where: { id: booking.id },
                    data: {
                        status: 'FAILED',
                        failureReason: error.response?.data?.message || error.message,
                    },
                });
            }

            // Release Redis locks (segment-aware)
            await releaseSeatLocks(scheduleId, sortedSeatIds, lockValue, fromSeq, toSeq);

            throw error;

        }
    }

    // ─── Verify Payment (client-side verification after Razorpay checkout) ───────
    async verifyPayment(bookingId, userId, razorpayPaymentId, razorpaySignature) {
        const booking = await prisma.booking.findUnique({
            where: { id: bookingId },
        });

        if (!booking || booking.userId !== userId) {
            throw new AppError('Booking not found', StatusCodes.NOT_FOUND);
        }

        if (!booking.paymentOrderId) {
            throw new AppError('Booking has no payment order', StatusCodes.BAD_REQUEST);
        }

        if (booking.status === 'CONFIRMED') {
            return { bookingId: booking.id, status: 'CONFIRMED', message: 'Already confirmed' };
        }

        if (booking.status !== 'PAYMENT_PENDING') {
            throw new AppError(`Booking is in ${booking.status} status, cannot verify payment`, StatusCodes.CONFLICT);
        }

        // Call payment service to verify and capture
        const result = await paymentClient.verifyPayment(
            booking.paymentOrderId,
            razorpayPaymentId,
            razorpaySignature
        );

        logger.info(`Payment verified for booking ${bookingId}`, { result });

        return {
            bookingId: booking.id,
            paymentStatus: result.status,
        };
    };


    // ─── Optimistic Lock Helper (CAS — Compare-And-Swap) ────────────────────────
    // Atomically updates booking status ONLY IF the version hasn't changed since read.
    // Returns the updated booking or throws StaleStateError if another process got there first.

    async casUpdateBooking(bookingId, expectedVersion, data) {
        const result = await prisma.booking.updateMany({
            where: { id: bookingId, version: expectedVersion },
            data: { ...data, version: { increment: 1 } },
        });

        if (result.count === 0) {
            throw new AppError(
                `Booking ${bookingId} was modified by another process (expected version ${expectedVersion})`,
                StatusCodes.CONFLICT
            );
        }
    };

    // ─── Notification Enrichment Helpers ─────────────────────────────────────────
    // Looks up user (and optionally stations) so booking events can carry email/firstName
    // directly. Failures here must never break the booking workflow — log and return null.

    async fetchUserForNotification(userId) {
        try {
            const user = await userClient.getUserById(userId);
            return user ? { email: user.email, firstName: user.firstName } : {};
        } catch (err) {
            logger.warn('Failed to enrich booking event with user details', {
                userId,
                error: err.message,
            });
            return {};
        }
    };

    async fetchStationName(stationId) {
        if (!stationId) return null;
        try {
            const station = await stationClient.getStationById(stationId);
            return station ? station.name : null;
        } catch (err) {
            logger.warn('Failed to enrich booking event with station name', {
                stationId,
                error: err.message,
            });
            return null;
        }
    };

    // ─── Handle Payment Success (Kafka consumer) ─────────────────────────────────

    async handlePaymentSuccess(paymentOrderId, gatewayPaymentId, amount) {
        const booking = await prisma.booking.findUnique({
            where: { paymentOrderId },
            include: { seats: true, passengers: true },
        });

        if (!booking) {
            logger.warn(`No booking found for paymentOrderId: ${paymentOrderId}`);
            return;
        }

        // Idempotent: already confirmed
        if (booking.status === 'CONFIRMED') {
            logger.info(`Booking ${booking.id} already confirmed`);
            return;
        }

        if (booking.status !== 'PAYMENT_PENDING') {
            logger.warn(`Booking ${booking.id} in unexpected status: ${booking.status}`);
            return;
        }

        const seatIds = booking.seats.map(s => s.seatId).sort();

        try {
            // Atomically claim this booking — if expiry job or cancel already changed it, bail out
            await casUpdateBooking(booking.id, booking.version, { status: 'CONFIRMING' });

            // Execute saga Step 3: Confirm seats in inventory
            await saga.executeConfirmSeats(booking, seatIds, booking.fromSeq, booking.toSeq); // --- SEGMENT BOOKING

            // Final status update (version was already incremented by CAS above)
            await prisma.booking.updateMany({
                where: { id: booking.id, status: 'CONFIRMING' },
                data: { status: 'CONFIRMED', version: { increment: 1 } },
            });

            // Release Redis locks (segment-aware)
            await forceReleaseSeatLocks(booking.scheduleId, seatIds, booking.fromSeq, booking.toSeq);

            // Publish BOOKING_CONFIRMED (retried by producer — log but don't fail the booking)
            try {
                const [userInfo, fromStationName, toStationName] = await Promise.all([
                    fetchUserForNotification(booking.userId),
                    fetchStationName(booking.fromStationId),
                    fetchStationName(booking.toStationId),
                ]);

                await bookingProducer.publishBookingConfirmed({
                    bookingId: booking.id,
                    userId: booking.userId,
                    email: userInfo.email,
                    firstName: userInfo.firstName,
                    scheduleId: booking.scheduleId,
                    trainNumber: booking.trainNumber,
                    trainName: booking.trainName,
                    fromStationName,
                    toStationName,
                    departureDate: booking.departureDate,
                    seats: booking.seats.map(s => ({
                        seatNumber: s.seatNumber,
                        seatType: s.seatType,
                        price: s.price,
                    })),
                    passengers: booking.passengers.map(p => ({
                        name: p.name,
                        age: p.age,
                        gender: p.gender,
                    })),
                    totalAmount: booking.totalAmount,
                });
            } catch (err) {
                logger.error('CRITICAL: Failed to publish BOOKING_CONFIRMED after retries — notification/search may be stale', {
                    bookingId: booking.id,
                    error: err.message,
                });
            }

            logger.info(`Booking ${booking.id} confirmed successfully`);

        } catch (error) {
            // If StaleStateError, another process already handled this booking — do nothing
            if (error.code === 'STALE_STATE') {
                logger.info(`Booking ${booking.id} already handled by another process, skipping`);
                return;
            }

            logger.error(`Failed to confirm booking ${booking.id}`, { error: error.message });

            // Compensate: refund payment and release seats
            await saga.compensateAll(booking, seatIds);

            await prisma.booking.updateMany({
                where: { id: booking.id, status: { in: ['PAYMENT_PENDING', 'CONFIRMING'] } },
                data: {
                    status: 'FAILED',
                    failureReason: `confirm_failed: ${error.message}`,
                    version: { increment: 1 },
                },
            });

            await forceReleaseSeatLocks(booking.scheduleId, seatIds, booking.fromSeq, booking.toSeq);

            try {
                const userInfo = await fetchUserForNotification(booking.userId);
                await bookingProducer.publishBookingFailed({
                    bookingId: booking.id,
                    userId: booking.userId,
                    email: userInfo.email,
                    firstName: userInfo.firstName,
                    scheduleId: booking.scheduleId,
                    reason: 'confirm_seats_failed',
                });
            } catch (err) {
                logger.error('Failed to publish BOOKING_FAILED after retries', { bookingId: booking.id, error: err.message });
            }
        }
    }

    // ─── Handle Payment Failure (Kafka consumer) ─────────────────────────────────

    async handlePaymentFailure(paymentOrderId, reason) {
        const booking = await prisma.booking.findUnique({
            where: { paymentOrderId },
            include: { seats: true },
        });

        if (!booking) {
            logger.warn(`No booking found for paymentOrderId: ${paymentOrderId}`);
            return;
        }

        // Idempotent
        if (booking.status === 'FAILED' || booking.status === 'CANCELLED' || booking.status === 'EXPIRED') {
            logger.info(`Booking ${booking.id} already in terminal state: ${booking.status}`);
            return;
        }

        if (booking.status !== 'PAYMENT_PENDING') {
            logger.warn(`Booking ${booking.id} in unexpected status: ${booking.status}`);
            return;
        }

        const seatIds = booking.seats.map(s => s.seatId).sort();

        // Atomically claim this booking before compensating
        try {
            await casUpdateBooking(booking.id, booking.version, {
                status: 'FAILED',
                failureReason: reason || 'payment_failed',
            });
        } catch (error) {
            if (error.code === 'STALE_STATE') {
                logger.info(`Booking ${booking.id} already handled by another process, skipping`);
                return;
            }
            throw error;
        }

        // Compensate: release held seats
        await saga.compensateHoldSeats(booking, seatIds);

        // Release Redis locks (segment-aware)
        await forceReleaseSeatLocks(booking.scheduleId, seatIds, booking.fromSeq, booking.toSeq);

        // Publish BOOKING_FAILED
        try {
            const userInfo = await fetchUserForNotification(booking.userId);
            await bookingProducer.publishBookingFailed({
                bookingId: booking.id,
                userId: booking.userId,
                email: userInfo.email,
                firstName: userInfo.firstName,
                scheduleId: booking.scheduleId,
                reason: reason || 'payment_failed',
            });
        } catch (err) {
            logger.error('Failed to publish BOOKING_FAILED after retries', { bookingId: booking.id, error: err.message });
        }

        logger.info(`Booking ${booking.id} failed: ${reason}`);
    };
    

    // ─── Cancel Booking ──────────────────────────────────────────────────────────

    async cancelBooking(bookingId, userId) {
        const booking = await prisma.booking.findUnique({
            where: { id: bookingId },
            include: { seats: true },
        });

        if (!booking) {
            throw new NotFoundError('Booking not found');
        }

        if (booking.userId !== userId) {
            throw new NotFoundError('Booking not found');
        }

        if (['CANCELLED', 'CANCELLING', 'FAILED', 'EXPIRED', 'CONFIRMING'].includes(booking.status)) {
            throw new ConflictError(`Booking is already ${booking.status}`);
        }

        const seatIds = booking.seats.map(s => s.seatId).sort();
        let refundInitiated = false;

        // Atomically claim this booking — prevents race with payment webhook or expiry job
        try {
            await casUpdateBooking(booking.id, booking.version, {
                status: 'CANCELLING',
                failureReason: 'user_cancelled',
            });
        } catch (error) {
            if (error.code === 'STALE_STATE') {
                // Re-read to give user accurate error
                const fresh = await prisma.booking.findUnique({ where: { id: bookingId } });
                throw new ConflictError(
                    `Booking status changed to ${fresh?.status || 'unknown'} while cancelling. Please refresh.`
                );
            }
            throw error;
        }

        if (booking.status === 'CONFIRMED') {
            // Cancel confirmed booking: release seats + refund
            try {
                await inventoryClient.cancelBooking(booking.scheduleId, booking.id, booking.userId);
            } catch (error) {
                logger.error(`Failed to release seats in inventory for booking ${booking.id}`, {
                    error: error.message,
                });
                // Roll back from CANCELLING to CONFIRMED so the user can retry
                await prisma.booking.updateMany({
                    where: { id: booking.id, status: 'CANCELLING' },
                    data: {
                        status: 'CONFIRMED',
                        failureReason: null,
                        version: { increment: 1 },
                    },
                });
                throw error;
            }

            if (booking.paymentOrderId) {
                try {
                    const idempotencyKey = `${booking.id}-cancel-refund`;
                    await paymentClient.initiateRefund(
                        booking.paymentOrderId,
                        booking.totalAmount,
                        'user_cancelled',
                        idempotencyKey
                    );
                    refundInitiated = true;
                } catch (error) {
                    logger.error(`Failed to initiate refund for booking ${booking.id}`, {
                        error: error.message,
                    });
                }
            }
        } else if (['PAYMENT_PENDING', 'SEATS_HELD'].includes(booking.status)) {
            // Release held seats
            try {
                // --- SEGMENT BOOKING: Pass segment params for accurate release ---
                await inventoryClient.releaseSeats(booking.scheduleId, seatIds, booking.userId, booking.fromSeq, booking.toSeq);
            } catch (error) {
                logger.error(`Failed to release seats during cancel`, { error: error.message });
            }
        }

        // Final status (CANCELLING → CANCELLED)
        await prisma.booking.updateMany({
            where: { id: booking.id, status: 'CANCELLING' },
            data: {
                status: 'CANCELLED',
                version: { increment: 1 },
            },
        });

        // Release Redis locks (segment-aware)
        await forceReleaseSeatLocks(booking.scheduleId, seatIds, booking.fromSeq, booking.toSeq);

        // Publish BOOKING_CANCELLED
        try {
            const userInfo = await fetchUserForNotification(booking.userId);
            await bookingProducer.publishBookingCancelled({
                bookingId: booking.id,
                userId: booking.userId,
                email: userInfo.email,
                firstName: userInfo.firstName,
                scheduleId: booking.scheduleId,
                reason: 'user_cancelled',
                refundAmount: refundInitiated ? booking.totalAmount : 0,
            });
        } catch (err) {
            logger.error('Failed to publish BOOKING_CANCELLED after retries', { bookingId: booking.id, error: err.message });
        }

        logger.info(`Booking ${booking.id} cancelled by user ${userId}`);

        return {
            bookingId: booking.id,
            status: 'CANCELLED',
            refundInitiated,
        };
    };


    // ─── Get Booking ─────────────────────────────────────────────────────────────
    async getBooking(bookingId, userId) {
        if (!bookingId) {
            throw new AppError("bookingId is required", StatusCodes.BAD_REQUEST);
        }

        const booking = await prisma.booking.findUnique({
            where: { id: bookingId },
            include: {
                seats: { orderBy: { seatNumber: 'asc' } },
                passengers: true,
            },
        });

        if (!booking || booking.userId !== userId) {
            throw new AppError('Booking not found', StatusCodes.NOT_FOUND);
        }

        return {
            id: booking.id,
            status: booking.status,
            scheduleId: booking.scheduleId,
            trainId: booking.trainId,
            trainNumber: booking.trainNumber,
            trainName: booking.trainName,
            departureDate: booking.departureDate,
            totalAmount: booking.totalAmount,
            seatCount: booking.seatCount,
            fromStationId: booking.fromStationId,  // --- SEGMENT BOOKING
            toStationId: booking.toStationId,      // --- SEGMENT BOOKING
            fromSeq: booking.fromSeq,              // --- SEGMENT BOOKING
            toSeq: booking.toSeq,                  // --- SEGMENT BOOKING
            paymentOrderId: booking.paymentOrderId,
            lockExpiresAt: booking.lockExpiresAt,
            failureReason: booking.failureReason,
            seats: booking.seats.map(s => ({
                seatId: s.seatId,
                seatNumber: s.seatNumber,
                seatType: s.seatType,
                price: s.price,
            })),
            passengers: booking.passengers.map(p => ({
                id: p.id,
                name: p.name,
                age: p.age,
                gender: p.gender,
                seatId: p.seatId,
            })),
            createdAt: booking.createdAt,
            updatedAt: booking.updatedAt,
        };
    }

    // ─── Get User Bookings ───────────────────────────────────────────────────────
    async getUserBookings(userId, { status, page = 1, limit = 10 } = {}) {
        if (!userId) {
            throw new AppError("userId is required", StatusCodes.BAD_REQUEST);
        }

        const parsedPage = Math.max(1, parseInt(page, 10) || 1);
        const parsedLimit = Math.max(1, parseInt(limit, 10) || 10);
        const skip = (parsedPage - 1) * parsedLimit;

        const where = { userId };
        if (status) {
            where.status = status.toUpperCase();
        }

        const [bookings, total] = await Promise.all([
            prisma.booking.findMany({
                where,
                include: {
                    seats: { orderBy: { seatNumber: 'asc' } },
                    passengers: true,
                },
                orderBy: { createdAt: 'desc' },
                skip,
                take: parsedLimit,
            }),
            prisma.booking.count({ where }),
        ]);

        return {
            bookings: bookings.map(b => ({
                id: b.id,
                status: b.status,
                scheduleId: b.scheduleId,
                trainNumber: b.trainNumber,
                trainName: b.trainName,
                departureDate: b.departureDate,
                totalAmount: b.totalAmount,
                seatCount: b.seatCount,
                fromStationId: b.fromStationId,  // --- SEGMENT BOOKING
                toStationId: b.toStationId,      // --- SEGMENT BOOKING
                fromSeq: b.fromSeq,              // --- SEGMENT BOOKING
                toSeq: b.toSeq,                  // --- SEGMENT BOOKING
                seats: b.seats.map(s => ({
                    seatId: s.seatId,
                    seatNumber: s.seatNumber,
                    seatType: s.seatType,
                    price: s.price,
                })),
                passengers: b.passengers.map(p => ({
                    id: p.id,
                    name: p.name,
                    age: p.age,
                    gender: p.gender,
                })),
                createdAt: b.createdAt,
            })),
            pagination: {
                page: parsedPage,
                limit: parsedLimit,
                total,
                totalPages: Math.ceil(total / parsedLimit) || 1,
            },
        };
    }
}

