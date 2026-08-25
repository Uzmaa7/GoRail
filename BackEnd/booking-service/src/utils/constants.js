const TOPICS = {
    // Booking topics (booking-service -> notification-service)
    BOOKING_CONFIRMED: 'booking.confirmed',
    BOOKING_CANCELLED: 'booking.cancelled',
    BOOKING_FAILED: 'booking.failed',

    // Consumer topics (subscribed by booking-service)
    PAYMENT_SUCCESS: 'payment.success',
    PAYMENT_FAILED: 'payment.failed',
    SCHEDULE_CANCELLED: 'schedule.cancelled',

    // DLQ Topic
    DLQ_BOOKING: 'booking.dlq',

};
export { TOPICS }