import { StatusCodes } from "http-status-codes";
import AppError from "../utils/errors/appError.js";
import { SuccessResponse, ErrorResponse } from "../utils/common/index.js";
import logger from "../config/logger.js";


export class UserController{
    constructor(userService){
        if(!userService){
            throw new Error("userService is required for UserController");
        }

        this.userService = userService;
    }

    async getProfile(req, res, next){
        try {

            const userId = req.user.id;

            if(!userId){
                throw new AppError("User is missing", StatusCodes.BAD_REQUEST);
            }

            const user = await this.userService.getProfile(userId);

            SuccessResponse.message = "User profile fetched Successfully";
            SuccessResponse.data = user;

            res
            .status(StatusCodes.OK)
            .json(SuccessResponse)


        } catch (error) {
            logger.error("Error in UserController [getProfile]:", error);

            ErrorResponse.error = error;

            return res
                .status(error.statusCode)
                .json(ErrorResponse)
        }
    }

    async getUserInternal(req, res, next) {
        try {
            const { userId } = req.params;

            if (!userId) {
                throw new AppError("User Id is missing", StatusCodes.BAD_REQUEST);
            }

            const user = await this.userService.getProfile(userId);

            if (!user) {
                throw new AppError("User not found", StatusCodes.NOT_FOUND);
            }

            SuccessResponse.message = "User internal details fetched successfully";
            SuccessResponse.data = {
                id: user.id,
                firstName: user.firstName,
                lastName: user.lastName,
                email: user.email,
            };

            return res
                .status(StatusCodes.OK)
                .json(SuccessResponse);

        } catch (error) {
            logger.error("Error in UserController [getUserInternal]:", error);
            ErrorResponse.error = error;

            return res
                .status(error.statusCode || StatusCodes.INTERNAL_SERVER_ERROR)
                .json(ErrorResponse);
        }
    }
}