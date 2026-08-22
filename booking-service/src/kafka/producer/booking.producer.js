import { producer, connectProducer } from "../../config/kafka.js"
import logger from '../../config/logger.js';
import { StatusCodes } from "http-status-codes";
import AppError from "../../utils/errors/appError.js"
import { TOPICS } from "../../utils/constants.js";

//Singleton design patter
//connecting booking-service to kafka server
const MAX_PUBLISH_RETRIES = 3;
const RETRY_DELAY_MS = 500;

class BookingProducer {
    constructor() {
        this.isInitialized = false;
    }

    async initialize() {
        if (!this.isInitialized) {
            await connectProducer();
            this.isInitialized = true;
        }
    }

    async sendMessage(topic, key, value) {

        //1. connect with kafka
        await this.initialize();

        let lastError;
        for (let attempt = 1; attempt <= MAX_PUBLISH_RETRIES; attempt++) {
            //2. create message
            try {
                const message = {
                    topic,
                    messages: [{
                        key: key || `${topic}-${Date.now()}`,
                        value: JSON.stringify(value),
                        timeStamp: Date.now().toString()
                    }]
                }

                //3. send msg to kafka(topic)
                const result = await producer.send(message);

                logger.info(`Message sent to kafka topic: ${topic}`, {
                    key,
                    partition: result[0].partition,
                    offset: result[0].offset,
                });

                return result;

            } catch (error) {

                logger.error(`[Kafka Failure] Error inside AdminProducer [sendMessage] for topic: ${topic}. Details: ${error.message}`, error);

                // Hum check kar sakte hain ki kya ye kafkajs ka connection/network error hai
                if (error.name === 'KafkaJSConnectionError' || error.message.includes('Connection timeout') || error.code === 'ECONNREFUSED') {
                    throw new AppError(
                        "Event streaming service is temporarily unavailable. Process aborted to maintain data integrity.",
                        StatusCodes.INTERNAL_SERVER_ERROR
                    );
                }

                if (error instanceof AppError) {
                    throw error;
                }
                lastError = error;

                if (attempt < MAX_PUBLISH_RETRIES) {
                    await new Promise(r => setTimeout(r, RETRY_DELAY_MS * attempt));
                }

                throw new AppError("Something went wrong while processing your request on the server", StatusCodes.INTERNAL_SERVER_ERROR);
            }
        }
        logger.error(`All ${MAX_PUBLISH_RETRIES} publish attempts failed for ${topic}`, { key });
        throw lastError;
    }

    async publishBookingConfirmed(data) {
        return this.sendMessage(
            TOPICS.BOOKING_CONFIRMED,
            `booking-${data.bookingId}`,
            { ...data, confirmedAt: new Date().toISOString() }
        );
    }

    async publishBookingCancelled(data) {
        return this.sendMessage(
            TOPICS.BOOKING_CANCELLED,
            `booking-${data.bookingId}`,
            { ...data, cancelledAt: new Date().toISOString() }
        );
    }

    async publishBookingFailed(data) {
        return this.sendMessage(
            TOPICS.BOOKING_FAILED,
            `booking-${data.bookingId}`,
            { ...data, failedAt: new Date().toISOString() }
        );
    }




}

export default new BookingProducer();