import { StatusCodes } from 'http-status-codes';
import config from '../config';
import AppError from '../utils/errors/appError.js';


/**
 * Validates that the request comes from an internal service
 * by checking the x-internal-service-key header.
 */
const internalAuth = (req, res, next) => {
     const serviceKey = req.headers['x-internal-service-key'];

     if (!serviceKey || serviceKey !== config.INTERNAL_SERVICE_KEY) {
          throw new AppError('Invalid or missing internal service key', StatusCodes.FORBIDDEN);
     }

     next();
};

export {internalAuth}