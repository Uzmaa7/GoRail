import { config } from '../config/index.js';
import AppError from '../utils/errors/appError.js';
import {StatusCodes} from "http-status-codes";

const internalAuth = (req, res, next) => {
     const serviceKey = req.headers['x-internal-service-key'];

     if (!serviceKey || serviceKey !== config.INTERNAL_SERVICE_KEY) {
          throw new AppError('Invalid or missing internal service key', StatusCodes.FORBIDDEN);
     }

     next();
};
export { internalAuth };