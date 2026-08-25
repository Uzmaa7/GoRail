import  express from 'express';
import  { internalAuth } from '../middlewares/internalAuth.middleware.js';
import {
     createPaymentOrder,
     getPaymentOrder,
     verifyAndCapturePayment,
     initiateRefund,
} from '../controllers/payment.controller.js';

const router = express.Router();

// Internal routes (called by booking-service)
router.post('/orders', internalAuth, createPaymentOrder);

router.post('/orders/:paymentOrderId/verify', internalAuth, verifyAndCapturePayment);

// router.get('/orders/:paymentOrderId', internalAuth, getPaymentOrder);
// router.post('/refunds', internalAuth, initiateRefund);

export default router