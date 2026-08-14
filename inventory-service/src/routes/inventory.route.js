import express from "express";
import { getUserContext } from "../middlewares/getUserContext.middleware.js";
import { config } from "../config/index.js";
import {
     getScheduleAvailability,
     getScheduleSeats,
     lockSeats,
     unlockSeats,
     confirmSeats,
     cancelBooking,
} from '../controllers/inventory.controller';
import { internalAuth } from "../middlewares/internalAuth.middleware.js";

const router = express.Router();



// Public: aggregate availability (used by search results)
router.get('/schedules/:scheduleId/availability', getScheduleAvailability);

// Authenticated OR internal: individual seat statuses
router.get('/schedules/:scheduleId/seats', userOrInternal, getScheduleSeats);

// Internal: called by booking-service (protected by service key)
router.post('/seats/lock', internalAuth, lockSeats);

router.post('/seats/confirm', internalAuth, confirmSeats);
router.post('/seats/cancel-booking', internalAuth, cancelBooking);

export default router;