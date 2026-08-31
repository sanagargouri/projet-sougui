import { Component, ElementRef, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../services/api.service';

@Component({
  selector: 'app-chatbot',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chatbot.component.html',
  styleUrls: ['./chatbot.component.scss']
})
export class ChatbotComponent {
  isOpen = false;
  userInput = '';
  loading = false;
  messages: { role: string; content: string }[] = [];

  @ViewChild('messagesContainer') messagesContainer!: ElementRef;

  constructor(private api: ApiService) {}

  toggle() {
    this.isOpen = !this.isOpen;
    if (this.isOpen && this.messages.length === 0) {
      this.messages.push({
        role: 'assistant',
        content: 'Bonjour ! Je suis l\'assistant IA de Sougui. Posez-moi vos questions sur nos produits, clients ou stratégie commerciale. 🏺'
      });
    }
  }

  send() {
    const text = this.userInput.trim();
    if (!text || this.loading) return;

    this.messages.push({ role: 'user', content: text });
    this.userInput = '';
    this.loading = true;
    this.scrollToBottom();

    const history = this.messages.map(m => ({ role: m.role, content: m.content }));

    this.api.chat(history).subscribe({
      next: (res) => {
        this.messages.push({ role: 'assistant', content: res.reply });
        this.loading = false;
        this.scrollToBottom();
      },
      error: (err) => {
        this.messages.push({
          role: 'assistant',
          content: '⚠️ Erreur de connexion. Vérifiez que le serveur est actif et la clé API configurée.'
        });
        this.loading = false;
        this.scrollToBottom();
      }
    });
  }

  private scrollToBottom() {
    setTimeout(() => {
      if (this.messagesContainer) {
        const el = this.messagesContainer.nativeElement;
        el.scrollTop = el.scrollHeight;
      }
    }, 50);
  }
}