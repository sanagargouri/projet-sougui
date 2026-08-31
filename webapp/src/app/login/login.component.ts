import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent {
  @Input() onLogin!: (user: any) => void;

  view: 'login' | 'forgot' | 'reset' = 'login';

  loginEmail = '';
  loginPassword = '';
  showPassword = false;

  forgotEmail = '';

  resetEmail = '';
  resetCode = '';
  resetPassword = '';
  resetConfirm = '';

  error = '';
  success = '';
  loading = false;

  constructor(private api: ApiService) {}

  login() {
    this.error = '';
    if (!this.loginEmail || !this.loginPassword) {
      this.error = 'Veuillez remplir tous les champs.';
      return;
    }
    this.loading = true;
    this.api.login(this.loginEmail, this.loginPassword).subscribe({
      next: (res) => {
        this.loading = false;
        localStorage.setItem('sougui_token', res.token);
        localStorage.setItem('sougui_user', JSON.stringify(res.user));
        this.onLogin(res.user);
      },
      error: (err) => {
        this.loading = false;
        this.error = err.error?.detail || 'Erreur de connexion.';
      }
    });
  }

  sendResetCode() {
    this.error = '';
    if (!this.forgotEmail) {
      this.error = 'Veuillez entrer votre adresse email.';
      return;
    }
    this.loading = true;
    this.api.forgotPassword(this.forgotEmail).subscribe({
      next: () => {
        this.loading = false;
        this.resetEmail = this.forgotEmail;
        this.success = 'Un code de réinitialisation a été envoyé à votre email.';
        this.view = 'reset';
      },
      error: (err) => {
        this.loading = false;
        this.error = err.error?.detail || "Erreur lors de l'envoi du code.";
      }
    });
  }

  resetPasswordSubmit() {
    this.error = '';
    if (!this.resetCode || !this.resetPassword || !this.resetConfirm) {
      this.error = 'Veuillez remplir tous les champs.';
      return;
    }
    if (this.resetPassword !== this.resetConfirm) {
      this.error = 'Les mots de passe ne correspondent pas.';
      return;
    }
    this.loading = true;
    this.api.resetPassword(this.resetEmail, this.resetCode, this.resetPassword).subscribe({
      next: () => {
        this.loading = false;
        this.success = 'Mot de passe réinitialisé ! Vous pouvez vous connecter.';
        this.view = 'login';
        this.loginEmail = this.resetEmail;
      },
      error: (err) => {
        this.loading = false;
        this.error = err.error?.detail || 'Code invalide ou expiré.';
      }
    });
  }

  goTo(view: 'login' | 'forgot' | 'reset') {
    this.view = view;
    this.error = '';
    this.success = '';
  }
}