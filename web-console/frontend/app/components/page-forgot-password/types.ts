

export interface IForgotPasswordData {
    email: string
}

export interface IForgotPasswordFormController extends ng.IFormController {
    email: ng.INgModelController
}
