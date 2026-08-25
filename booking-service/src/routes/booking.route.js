import express from 'express';
import initializedContainer from "../dependencies/dependency.js";
import { getUserContext } from '../middlewares/getUserContext.middleware.js';

const router = express.Router();

const {bookingController} = initializedContainer.controller;

// All booking routes require authentication (user context from gateway)
router.post('/bookings', getUserContext,  (req, res, next) => bookingController.createBooking(req, res, next));

router.get('/bookings', getUserContext, (req, res, next) => bookingController.getUserBookings(req, res, next));

router.get('/bookings/:bookingId', getUserContext, (req, res, next) => bookingController.getBooking(req, res, next));

router.post('/bookings/:bookingId/verify-payment', getUserContext, (req, res, next) => bookingController.verifyPayment(req, res, next));

router.post('/bookings/:bookingId/cancel', getUserContext, (req, res, next) => bookingController.cancelBooking(req, res, next));

export default router;