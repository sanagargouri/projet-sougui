import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
@Injectable({ providedIn: 'root' })
export class ApiService {
  private baseUrl = 'http://localhost:8001';
  constructor(private http: HttpClient) {}
  // ── Auth ──────────────────────────────────────
  login(email: string, password: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/auth/login`, { email, password });
  }
  forgotPassword(email: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/auth/forgot-password`, { email });
  }
  resetPassword(email: string, code: string, new_password: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/auth/reset-password`, { email, code, new_password });
  }
  getMe(): Observable<any> {
    const token = localStorage.getItem('sougui_token');
    const headers = new HttpHeaders({ Authorization: `Bearer ${token}` });
    return this.http.get(`${this.baseUrl}/auth/me`, { headers });
  }
  // ── ML Predictions ────────────────────────────
  predict(model: string, data: any[]): Observable<any> {
    return this.http.post(`${this.baseUrl}/predict/${model}`, { data });
  }
  // ── Chat ──────────────────────────────────────
  chat(messages: { role: string; content: string }[]): Observable<{ reply: string }> {
    return this.http.post<{ reply: string }>(`${this.baseUrl}/chat`, { messages });
  }
  // ── ETL ───────────────────────────────────────
  runEtl(): Observable<any> {
    return this.http.post(`${this.baseUrl}/run/etl`, {});
  }
  // ── Admin Users ───────────────────────────────
  getUsers(): Observable<any> {
    const token = localStorage.getItem('sougui_token');
    const headers = new HttpHeaders({ Authorization: `Bearer ${token}` });
    return this.http.get(`${this.baseUrl}/admin/users`, { headers });
  }
  adminCreateUser(user: { username: string; email: string; password: string; role: string }): Observable<any> {
    const token = localStorage.getItem('sougui_token');
    const headers = new HttpHeaders({ Authorization: `Bearer ${token}` });
    return this.http.post(`${this.baseUrl}/admin/users`, user, { headers });
  }
  deleteUser(id: number): Observable<any> {
    const token = localStorage.getItem('sougui_token');
    const headers = new HttpHeaders({ Authorization: `Bearer ${token}` });
    return this.http.delete(`${this.baseUrl}/admin/users/${id}`, { headers });
  }
  // ── Extraction Facture ────────────────────────
  extractFacture(file: any): Observable<Blob> {
    const token = localStorage.getItem('sougui_token');
    const headers = new HttpHeaders({ Authorization: `Bearer ${token}` });
    const formData = new FormData();
    formData.append('file', file);
    return this.http.post(`${this.baseUrl}/extract/facture`, formData, { headers, responseType: 'blob' });
  }
  // ── Health ────────────────────────────────────
  health(): Observable<any> {
    return this.http.get(`${this.baseUrl}/health`);
  }
}
