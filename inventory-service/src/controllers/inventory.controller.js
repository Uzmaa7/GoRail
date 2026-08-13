
import inventoryService from "../services/inventory.service.js";
import AppError from "../utils/errors/appError.js";
import { StatusCodes } from "http-status-codes";
import { SuccessResponse, ErrorResponse } from "../utils/common/index.js";
import logger from "../config/logger.js";


export const getScheduleAvailability = async (req, res, next) => {
    try {
        const { scheduleId } = req.params;

        if (!scheduleId) {
            throw new AppError("Schedule ID is required in request parameters", StatusCodes.BAD_REQUEST);
        }

        const data = await inventoryService.getAvailability(scheduleId);

        SuccessResponse.data = data;
        SuccessResponse.message = "Schedule availability fetched successfully";

        return res
            .status(StatusCodes.OK)
            .json(SuccessResponse);

    } catch (error) {
        logger.error("Error in getScheduleAvailability controller : ", error);

        const statusCode = error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR;
        ErrorResponse.error = error;

        return res
            .status(statusCode)
            .json(ErrorResponse);
    }
};


export const getScheduleSeats = async (req, res, next) => {
    try {
        const { scheduleId } = req.params;
        const { status, seatType, fromSeq, toSeq } = req.query; // --- SEGMENT BOOKING: added fromSeq/toSeq

        if (!scheduleId) {
            throw new AppError("Schedule ID is required in request parameters", StatusCodes.BAD_REQUEST);
        }

        const filters = {};
        if (status) filters.status = status.toUpperCase();
        if (seatType) filters.seatType = seatType.toUpperCase();
        if (fromSeq) filters.fromSeq = fromSeq; // --- SEGMENT BOOKING
        if (toSeq) filters.toSeq = toSeq;       // --- SEGMENT BOOKING

        const data = await inventoryService.getSeats(scheduleId, filters);

        SuccessResponse.data = data;
        SuccessResponse.message = "Schedule seats fetched successfully";

        return res
            .status(StatusCodes.OK)
            .json(SuccessResponse);

    } catch (error) {
        logger.error("Error in getScheduleSeats controller : ", error);

        const statusCode = error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR;
        ErrorResponse.error = error;

        return res
            .status(statusCode)
            .json(ErrorResponse);
    }
};


export const lockSeats = async (req, res, next) => {
    try {
        const { scheduleId, seatIds, ttlSeconds, userId, fromSeq, toSeq } = req.body; // --- SEGMENT BOOKING: added fromSeq/toSeq

        if (!scheduleId || !seatIds || !Array.isArray(seatIds) || seatIds.length === 0) {
            throw new AppError("scheduleId and seatIds (non-empty array) are required", StatusCodes.BAD_REQUEST);
        }

        if (!userId) {
            throw new AppError("userId is required", StatusCodes.BAD_REQUEST);
        }

        const result = await inventoryService.lockSeats(
            scheduleId, 
            seatIds, 
            userId, 
            ttlSeconds, 
            fromSeq, // --- SEGMENT BOOKING
            toSeq
        );

        SuccessResponse.data = {
            scheduleId: result.scheduleId,
            lockedSeats: result.lockedSeats,
            lockExpiresAt: result.lockExpiresAt,
        };
        SuccessResponse.message = `${result.lockedSeats.length} seat(s) locked successfully`;

        return res
            .status(StatusCodes.OK)
            .json(SuccessResponse);

    } catch (error) {
        logger.error("Error in lockSeats controller : ", error);

        const statusCode = error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR;
        ErrorResponse.error = error;

        return res
            .status(statusCode)
            .json(ErrorResponse);
    }
};





export const confirmSeats = async (req, res, next) => {
    try {
        const { scheduleId, seatIds, bookingId, userId, fromSeq, toSeq } = req.body; // --- SEGMENT BOOKING: added fromSeq/toSeq

        if (!scheduleId || !seatIds || !Array.isArray(seatIds) || seatIds.length === 0) {
            throw new AppError("scheduleId and seatIds (non-empty array) are required", StatusCodes.BAD_REQUEST);
        }

        if (!bookingId) {
            throw new AppError("bookingId is required", StatusCodes.BAD_REQUEST);
        }

        if (!userId) {
            throw new AppError("userId is required", StatusCodes.BAD_REQUEST);
        }

        const result = await inventoryService.confirmSeats(
            scheduleId, 
            seatIds, 
            userId, 
            bookingId, 
            fromSeq, 
            toSeq
        );

        SuccessResponse.data = {
            scheduleId: result.scheduleId,
            bookingId: result.bookingId,
            confirmedSeats: result.confirmedSeats,
        };
        SuccessResponse.message = `${result.confirmedSeats.length} seat(s) confirmed`;

        return res
            .status(StatusCodes.OK)
            .json(SuccessResponse);

    } catch (error) {
        logger.error("Error in confirmSeats controller : ", error);

        const statusCode = error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR;
        ErrorResponse.error = error;

        return res
            .status(statusCode)
            .json(ErrorResponse);
    }
};



export const cancelBooking = async (req, res, next) => {
    try {
        const { scheduleId, bookingId, userId } = req.body;

        if (!scheduleId || !bookingId) {
            throw new AppError("scheduleId and bookingId are required", StatusCodes.BAD_REQUEST);
        }

        if (!userId) {
            throw new AppError("userId is required", StatusCodes.BAD_REQUEST);
        }

        const result = await inventoryService.cancelBooking(scheduleId, bookingId, userId);

        SuccessResponse.data = {
            scheduleId: result.scheduleId,
            bookingId: result.bookingId,
            releasedSeats: result.releasedSeats,
        };
        SuccessResponse.message = `Booking cancelled, ${result.releasedSeats.length} seat(s) released`;

        return res
            .status(StatusCodes.OK)
            .json(SuccessResponse);

    } catch (error) {
        logger.error("Error in cancelBooking controller : ", error);

        const statusCode = error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR;
        ErrorResponse.error = error;

        return res
            .status(statusCode)
            .json(ErrorResponse);
    }
};