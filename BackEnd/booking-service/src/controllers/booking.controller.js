import { StatusCodes } from "http-status-codes";
import AppError from "../utils/errors/appError.js";
import { SuccessResponse, ErrorResponse } from "../utils/common/index.js";
import logger from "../config/logger.js";

export class BookingController {
    constructor(bookingService) {
        if (!bookingService) {
            throw new Error("bookingService is required for BookingController");
        }

        this.bookingService = bookingService;
    }

    async createBooking(req, res, next) {

        try {

            const userId = req.user.id;

            const { scheduleId, seatIds, passengers, idempotencyKey, fromStationId, toStationId, fromSeq, toSeq } = req.body;

            if (!scheduleId || !seatIds || !passengers || !idempotencyKey) {
                throw new AppError('scheduleId, seatIds, passengers, and idempotencyKey are required', StatusCodes.BAD_REQUEST);
            }

            const result = await this.bookingService.createBooking(
                userId, scheduleId, seatIds, passengers, idempotencyKey,
                fromStationId, toStationId, fromSeq, toSeq
            );

            SuccessResponse.message = "Booking Created Successfully";
            SuccessResponse.data = result;

            return res
                .status(StatusCodes.CREATED)
                .json(SuccessResponse)



        } catch (error) {
            logger.error("Error in BookingController [createBooking]:", error);

            ErrorResponse.error = error;

            return res
                .status(error.statusCode)
                .json(ErrorResponse)
        }
    }

    async verifyPayment(req, res, next) {
        try {
            const userId = req.user.id;
            const { bookingId } = req.params;
            const { razorpayPaymentId, razorpaySignature } = req.body;

            if (!razorpayPaymentId || !razorpaySignature) {
                throw new AppError('razorpayPaymentId and razorpaySignature are required', StatusCodes.BAD_REQUEST);
            }

            const result = await this.bookingService.verifyPayment(bookingId, userId, razorpayPaymentId, razorpaySignature);

            SuccessResponse.message = "Payment Verified Successfully";
            SuccessResponse.data = result;

            return res
                .status(StatusCodes.OK)
                .json(SuccessResponse)

        } catch (error) {
            logger.error("Error in BookingController [verifyPayment]:", error);

            ErrorResponse.error = error;

            return res
                .status(error.statusCode)
                .json(ErrorResponse)
        }



    };

    async cancelBooking(req, res, next) {
        try {
            const userId = req.user.id;
            const { bookingId } = req.params;

            if (!bookingId) {
                throw new AppError('bookingId is required', StatusCodes.BAD_REQUEST);
            }

            const result = await this.bookingService.cancelBooking(bookingId, userId);

            SuccessResponse.message = "Booking Cancelled Successfully";
            SuccessResponse.data = result;

            return res
                .status(StatusCodes.OK)
                .json(SuccessResponse);

        } catch (error) {
            logger.error("Error in BookingController [cancelBooking]:", error);

            ErrorResponse.error = error;

            return res
                .status(error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR)
                .json(ErrorResponse);
        }
    };

    async getBooking(req, res, next) {
        try {
            const userId = req.user.id;
            const { bookingId } = req.params;

            if (!bookingId) {
                throw new AppError('bookingId is required', StatusCodes.BAD_REQUEST);
            }

            const result = await this.bookingService.getBooking(bookingId, userId);

            SuccessResponse.message = "Booking Details Fetched Successfully";
            SuccessResponse.data = result;

            return res
                .status(StatusCodes.OK)
                .json(SuccessResponse);

        } catch (error) {
            logger.error("Error in BookingController [getBookingDetails]:", error);

            ErrorResponse.error = error;

            return res
                .status(error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR)
                .json(ErrorResponse);
        }
    }

    async getUserBookings(req, res, next) {
        try {
            const userId = req.user.id;
            const { status, page, limit } = req.query;

            const result = await this.bookingService.getUserBookings(userId, {
                status,
                page: page ? parseInt(page, 10) : 1,
                limit: limit ? parseInt(limit, 10) : 10,
            });

            SuccessResponse.message = "User Bookings Fetched Successfully";
            SuccessResponse.data = result;

            return res
                .status(StatusCodes.OK)
                .json(SuccessResponse);

        } catch (error) {
            logger.error("Error in BookingController [getUserBookings]:", error);

            ErrorResponse.error = error;

            return res
                .status(error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR)
                .json(ErrorResponse);
        }
    }


}