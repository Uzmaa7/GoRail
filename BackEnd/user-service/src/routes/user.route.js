import express from "express";
import initializedContainer from "../dependencies/dependency.js";
// import { verifyJWT } from "../middlewares/auth.middleware.js";
import {getUserContext} from "../middlewares/getUserContext.middleware.js";
import { internalAuth } from "../middlewares/internalAuth.middleware.js";

const {userController} = initializedContainer.controller;

const router = express.Router();

router.get("/profile", getUserContext, (req, res, next) => userController.getProfile(req, res, next));

router.get("/internal/:userId", internalAuth, (req, res, next) => userController.getUserInternal(req, res, next));

export default router;