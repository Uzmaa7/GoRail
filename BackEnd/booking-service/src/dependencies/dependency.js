import bookingRepository from "../repositories/bookingRepository.js";
import idempotencyRepository from "../repositories/idempotencyRepository.js";
import { BookingService } from "../services/booking.service.js";
import { BookingController } from "../controllers/booking.controller.js";

/**
 * Dependency Injection Container for the Booking module.
 * This container initializes and manages the dependencies for the Booking module,
 * including repositories, services, and controllers.
 */
class Container {
    static init() {
        // Initialize repositories
        const repositories = {
            bookingRepository: bookingRepository,
            idempotencyRepository: idempotencyRepository
        };

        // Initialize services with their respective repositories
        const services = {
            bookingService: new BookingService(repositories.bookingRepository),
        };

        // Initialize controllers with their respective services
        const controller = {
            bookingController: new BookingController(services.bookingService),
        }

        return {
            repositories, services, controller
        }
    }
}

const initialized = Container.init();
export { Container };
export default initialized