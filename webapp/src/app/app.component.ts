import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService } from './services/api.service';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { LoginComponent } from './login/login.component';
import { ChatbotComponent } from './chatbot/chatbot.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule, LoginComponent, ChatbotComponent],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
  activeTab = 'home';
  loading = false;
  result: any = null;
  error: string = '';
  showHistory = false;
  history: any[] = [];
  isLoggedIn = false;
  currentUser: any = null;

  // Role access
  roleAccess: { [role: string]: string[] } = {
    admin:     ['extraction', 'classification', 'regression', 'clustering', 'timeseries', 'dashboard', 'users'],
    marketing: ['classification', 'clustering', 'dashboard'],
    technique: ['etl', 'monitoring'],
  };

  canAccess(tab: string): boolean {
    if (!this.currentUser) return false;
    const role = this.currentUser.role || 'marketing';
    return this.roleAccess[role]?.includes(tab) ?? false;
  }

  pbiAccess: { [role: string]: string[] } = {
    admin:     ['home', 'overview', 'expenses', 'products', 'clients', 'concurrents', 'fournisseurs'],
    marketing: ['home', 'products', 'clients', 'concurrents'],
    technique: ['home', 'overview', 'expenses', 'fournisseurs'],
  };

  getVisiblePbiPages() {
    if (!this.currentUser) return [];
    const role = this.currentUser.role || 'marketing';
    const allowed = this.pbiAccess[role] || [];
    return this.pbiPages.filter(p => allowed.includes(p.id));
  }


  getPbiBaseUrl(): string {
    const role = this.currentUser?.role || 'marketing';
    return this.pbiUrls[role] || this.pbiUrls['admin'];
  }
  // Animated counter values
  animatedMontant = 0;
  animatedCA = 0;
  animatedConfidence = 0;

  // ETL
  etlLoading = false;
  etlResult: any = null;
  etlError: string = '';

  // Extraction
  extractionFile: File | null = null;
  extractionLoading = false;
  extractionError = "";
  extractionSuccess = "";

  // Monitoring
  activeMonitor = 'grafana';
  grafanaUrl: SafeResourceUrl;
  kibanaUrl: SafeResourceUrl;

  // Users management
  showAddUser = false;
  usersList: any[] = [];
  newUser = { username: '', email: '', password: '', role: 'marketing' };
  userSuccess = '';
  userError = '';

  pbiUrls: { [role: string]: string } = {
    admin: 'https://app.powerbi.com/reportEmbed?reportId=b3181b1d-4f60-463b-9179-5c9a1d4ef717&autoAuth=true&ctid=604f1a96-cbe8-43f8-abbf-f8eaf5d85730&navContentPaneEnabled=false&filterPaneEnabled=false&chromeless=true',
    marketing: 'https://app.powerbi.com/reportEmbed?reportId=392db0a4-42a6-43ba-8817-2c6c02d6f217&autoAuth=true&ctid=604f1a96-cbe8-43f8-abbf-f8eaf5d85730&navContentPaneEnabled=false&filterPaneEnabled=false&chromeless=true',
  };
  activePbiPage = 'home';
  pbiUrl: SafeResourceUrl;

  pbiPages = [
    { id: 'home',         label: 'Home',                pageId: '6a38610de2effed99a21' },
    { id: 'overview',     label: 'Overview',             pageId: 'ee015a31dbc7ee088b39' },
    { id: 'expenses',     label: 'Expenses & Sales',      pageId: '2bd135f8a168e9026f70' },
    { id: 'products',     label: 'Performance Produits',  pageId: 'ba1ccf94abc305f35eec' },
    { id: 'clients',      label: 'Clients',               pageId: '0b7f5654e7f655a523f4' },
    { id: 'concurrents',  label: 'Concurrents',           pageId: 'ca68cf206926d86d9b7f' },
    { id: 'fournisseurs', label: 'Fournisseurs',          pageId: '7d497b89dc250554049d' },
  ];

  villes = [
    'TUNIS','SFAX','SOUSSE','KAIROUAN','BIZERTE','GABES','ARIANA','GAFSA',
    'MONASTIR','HAMMAMET','BEN AROUS','MEDENINE','NABEUL','TOZEUR','SIDI BOUZID',
    'JENDOUBA','BEJA','EL KEF','MAHDIA','KASSERINE','ZARZIS','HOUMT EL SOUK',
    'MSAKEN','KELIBIA','RAADES','LA GOULLETTE','DOUZ','DJERBA','EL DJEM','AKOUDA',
    'BENI KHALED','BENI GARDANE','BIZERTE PLAGE','BOUFICHA','FOUCHANA','LA MARSA',
    'LA SOUKRA','SAKHIET EZZIT','HANNAM SOUSSE','KHERKOUANE','TABARKA','HAIDRA',
    'FARIANA','MORNAGUIA'
  ];

  villesRegress = [
    'Ariana','Beja','Ben Arous','Bizerte','Gabes','Gafsa','Jendouba',
    'Kairouan','Kebili','Le Kef','Mahdia','Manouba','Medenine',
    'Monastir','Nabeul','Sfax','Sidi Bouzid','Siliana','Sousse',
    'Tataouine','Tozeur','Tunis','Zaghouan'
  ];

  classif = {
    Client_PK: 1, recence_jours: 30, panier_moyen: 250,
    ca_saison_haute: 1200, montant_total: 3000,
    nb_factures_b2b: 0, nb_commandes_b2c: 5, nb_commandes: 5,
    Type_client: 'B2C', ville_client: 'TUNIS',
    paiement_dominant: 'carte', type_vente_dominant: 'en_ligne'
  };

  regress = {
    prix_catalogue: 170.0,
    ville_client: 'Tunis',
    categorie: 'ARTISANAT',
    type_paiement: 'PAY IN PERSON',
    En_Promo: 0,
    a_code_promo: 0,
    Mois: 5,
    est_saison_haute: 0,
  };

  cluster = {
    Recency: 30, Frequency: 8, Monetary: 3000.0, Avg_Basket: 375.0,
    Total_Quantite: 20, Nb_Produits: 5, Total_Remise: 150.0,
    Total_Remboursement: 0.0, Nb_Ventes: 8, Mode_Vente_Principal: 'en_ligne',
    Taux_Remise: 0.05, Is_Entreprise: 0
  };

 tsMonths = [
    { label: 'Il y a 4 mois', Date: '2024-01-01', CA_Mensuel: 1200, Quantite_Mensuelle: 50, Nb_Commandes: 15, Remise_Totale: 80, Prix_Moyen_Unite: 70 },
    { label: 'Il y a 3 mois', Date: '2024-02-01', CA_Mensuel: 1400, Quantite_Mensuelle: 60, Nb_Commandes: 18, Remise_Totale: 100, Prix_Moyen_Unite: 72 },
    { label: 'Il y a 2 mois', Date: '2024-03-01', CA_Mensuel: 1600, Quantite_Mensuelle: 65, Nb_Commandes: 20, Remise_Totale: 120, Prix_Moyen_Unite: 75 },
    { label: 'Mois dernier',  Date: '2024-04-01', CA_Mensuel: 1800, Quantite_Mensuelle: 70, Nb_Commandes: 22, Remise_Totale: 140, Prix_Moyen_Unite: 78 },
];

  constructor(private api: ApiService, private sanitizer: DomSanitizer) {
    this.pbiUrl = this.sanitizer.bypassSecurityTrustResourceUrl(
      this.getPbiBaseUrl() + '&pageName=' + this.pbiPages[0].pageId
    );
    this.grafanaUrl = this.sanitizer.bypassSecurityTrustResourceUrl('http://PLACEHOLDER_IP:3000');
    this.kibanaUrl = this.sanitizer.bypassSecurityTrustResourceUrl('http://PLACEHOLDER_IP:5601');
  }

  ngOnInit() {
    const savedUser = localStorage.getItem('sougui_user');
    const savedToken = localStorage.getItem('sougui_token');
    if (savedUser && savedToken) {
      try {
        this.currentUser = JSON.parse(savedUser);
        this.isLoggedIn = true;
      } catch {
        this.clearSession();
      }
    }
    const saved = localStorage.getItem('sougui_history');
    if (saved) this.history = JSON.parse(saved);
  }

  onLogin = (user: any) => {
    this.isLoggedIn = true;
    this.currentUser = user;
    this.pbiUrl = this.sanitizer.bypassSecurityTrustResourceUrl(this.getPbiBaseUrl() + '&pageName=' + this.pbiPages[0].pageId);
  }

  logout() {
    this.clearSession();
    this.isLoggedIn = false;
    this.currentUser = null;
    this.result = null;
    this.activeTab = 'home';
  }

  private clearSession() {
    localStorage.removeItem('sougui_token');
    localStorage.removeItem('sougui_user');
  }

  setTab(tab: string) {
    this.activeTab = tab;
    this.result = null;
    this.error = '';
    if (tab === 'users') this.loadUsers();
  }

  setPbiPage(page: any) {
    this.activePbiPage = page.id;
    this.pbiUrl = this.sanitizer.bypassSecurityTrustResourceUrl(
      this.getPbiBaseUrl() + '&pageName=' + page.pageId
    );
  }

  onTypeClientChange() {
    if (this.classif.Type_client === 'B2C') {
      this.classif.nb_factures_b2b = 0;
      this.classif.nb_commandes = this.classif.nb_commandes_b2c;
    } else {
      this.classif.nb_commandes_b2c = 0;
      this.classif.nb_commandes = this.classif.nb_factures_b2b;
    }
  }

  animateCounter(target: number, field: 'animatedMontant' | 'animatedCA' | 'animatedConfidence', duration = 1500) {
    const start = 0;
    const startTime = performance.now();
    const step = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress);
      this[field] = Math.round(start + (target - start) * eased);
      if (progress < 1) { requestAnimationFrame(step); } else { this[field] = Math.round(target); }
    };
    requestAnimationFrame(step);
  }

  startResultAnimations() {
    if (!this.result?.predictions?.length) return;
    const p = this.result.predictions[0];
    if (this.activeTab === 'classification' && p.probabilite !== undefined) {
      this.animatedConfidence = 0;
      this.animateCounter(Math.round(p.probabilite * 100), 'animatedConfidence', 1200);
    }
    if (this.activeTab === 'regression' && p.montant_predit !== undefined) {
      this.animatedMontant = 0;
      this.animateCounter(Math.round(p.montant_predit), 'animatedMontant', 1500);
    }
    if (this.activeTab === 'timeseries' && p.ca_predit !== undefined) {
      this.animatedCA = 0;
      this.animateCounter(Math.round(p.ca_predit), 'animatedCA', 1800);
    }
  }

  getSegmentDescription(label: string): string {
    const d: any = {
      'B2C Dormants':            'Client particulier inactif depuis longtemps. Recommandez une campagne de réactivation avec une offre spéciale.',
      'B2B Actifs Moyens':       'Entreprise cliente régulière avec un volume moyen. Potentiel de croissance avec un suivi commercial dédié.',
      'B2C Remisés (à risque)':  "Client qui achète principalement en promotion. Risque de désengagement si les remises s'arrêtent.",
      'B2B Grands Comptes':      'Client entreprise stratégique à très forte valeur. Priorité absolue pour la fidélisation.',
      'B2C Récents Actifs':      'Nouveau client très actif et prometteur. Moment idéal pour renforcer la relation commerciale.',
    };
    return d[label] || 'Profil client identifié avec succès.';
  }

  getRecommendation(): string {
    if (!this.result?.predictions?.length) return '';
    const p = this.result.predictions[0];
    if (this.activeTab === 'classification') {
      if (p.prediction === 1) return 'Recommandation : Proposez à ce client une carte de fidélité ou une offre VIP pour renforcer sa fidélité.';
      return 'Recommandation : Contactez ce client avec une offre de réengagement — remise exclusive ou programme de points.';
    }
    if (this.activeTab === 'regression') {
      if (p.montant_predit > 200) return 'Recommandation : Commande à haute valeur. Proposez la livraison gratuite pour finaliser la vente.';
      return 'Recommandation : Proposez un produit complémentaire pour augmenter la valeur du panier.';
    }
    if (this.activeTab === 'clustering') {
      const recs: any = {
        'B2C Dormants':           'Recommandation : Envoyez un email de réactivation avec une remise de 15% valable 7 jours.',
        'B2B Actifs Moyens':      'Recommandation : Assignez un commercial dédié pour développer ce compte.',
        'B2C Remisés (à risque)': 'Recommandation : Réduisez progressivement les remises et mettez en avant la valeur produit.',
        'B2B Grands Comptes':     'Recommandation : Planifiez une réunion stratégique trimestrielle avec ce client prioritaire.',
        'B2C Récents Actifs':     'Recommandation : Envoyez un programme de bienvenue et présentez la gamme complète.',
      };
      return recs[p.Segment_Label] || 'Analysez ce profil pour adapter votre stratégie commerciale.';
    }
    if (this.activeTab === 'timeseries') {
      const last = this.tsMonths[this.tsMonths.length - 1].CA_Mensuel;
      if (p.ca_predit > last) return 'Recommandation : Croissance prévue — renforcez les stocks et préparez l\'équipe commerciale.';
      return 'Recommandation : Baisse prévue — lancez une campagne promotionnelle pour stimuler les ventes.';
    }
    return '';
  }

  saveToHistory(res: any) {
    const p = res.predictions[0];
    let label = '';
    let confidence = null;
    if (this.activeTab === 'classification') {
      label = p.prediction === 1 ? 'Fidèle' : 'À risque';
      confidence = p.probabilite;
    } else if (this.activeTab === 'regression') {
      label = `${p.montant_predit} TND`;
    } else if (this.activeTab === 'clustering') {
      label = `${p.Segment_Label}`;
    } else {
      label = `${p.ca_predit} TND`;
    }
    const entry = {
      date: new Date().toLocaleString('fr-FR'),
      type: this.getTabLabel(),
      result: label,
      confidence,
      duration: res.duration_ms
    };
    this.history.unshift(entry);
    if (this.history.length > 50) this.history = this.history.slice(0, 50);
    localStorage.setItem('sougui_history', JSON.stringify(this.history));
  }

  getTabLabel(): string {
    const labels: any = {
      classification: 'Fidélité Client',
      regression: 'Valeur Commande',
      clustering: 'Profil Client',
      timeseries: 'Prévision CA'
    };
    return labels[this.activeTab] || this.activeTab;
  }

  clearHistory() {
    this.history = [];
    localStorage.removeItem('sougui_history');
  }

  exportPDF() {
    const win = window.open('', '_blank');
    if (!win) return;
    const rows = this.history.map(h => `
      <tr>
        <td>${h.date}</td>
        <td>${h.type}</td>
        <td>${h.result}</td>
        <td>${h.confidence !== null && h.confidence !== undefined ? (h.confidence * 100).toFixed(0) + '%' : '—'}</td>
        <td>${h.duration} ms</td>
      </tr>`).join('');
    win.document.write(`
      <!DOCTYPE html><html><head><meta charset="utf-8">
      <title>Historique — SOUGUI</title>
      <style>
        body { font-family: 'Segoe UI', sans-serif; padding: 40px; color: #2c3e50; }
        h1 { color: #1E2D6B; border-bottom: 3px solid #E044A7; padding-bottom: 12px; }
        .subtitle { color: #888; margin-bottom: 30px; font-size: 13px; }
        table { width: 100%; border-collapse: collapse; }
        th { background: #1E2D6B; color: white; padding: 12px; text-align: left; }
        td { padding: 10px 12px; border-bottom: 1px solid #dde3f0; font-size: 13px; }
        tr:hover td { background: #f0f4ff; }
        .footer { margin-top: 30px; color: #aaa; font-size: 11px; text-align: center; }
      </style></head><body>
      <h1>SOUGUI — Historique des Prédictions IA</h1>
      <div class="subtitle">Exporté le ${new Date().toLocaleString('fr-FR')} · ${this.history.length} analyses</div>
      <table>
        <thead><tr><th>Date</th><th>Type</th><th>Résultat</th><th>Confiance</th><th>Durée</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
      <div class="footer">Plateforme IA Sougui — Document Confidentiel</div>
      </body></html>`);
    win.document.close();
    setTimeout(() => win.print(), 500);
  }

  predict() {
    this.loading = true;
    this.result = null;
    this.error = '';
    let data: any[];
    let model: string;

    if (this.activeTab === 'classification') {
      model = 'classification';
      data = [this.classif];
    } else if (this.activeTab === 'regression') {
      model = 'regression';
      const mois = Number(this.regress.Mois);
      data = [{
        prix_catalogue:   this.regress.prix_catalogue,
        ville_client:     this.regress.ville_client,
        categorie:        this.regress.categorie,
        type_paiement:    this.regress.type_paiement,
        En_Promo:         this.regress.En_Promo,
        a_code_promo:     this.regress.a_code_promo,
        canal_produit:    'Site Web',
        Annee:            new Date().getFullYear(),
        Mois:             mois,
        Trimestre:        `Trimestre ${Math.ceil(mois / 3)}`,
        Semestre:         `Semestre ${mois <= 6 ? 1 : 2}`,
        est_saison_haute: [6,7,8,12].includes(mois) ? 1 : 0,
      }];
    } else if (this.activeTab === 'clustering') {
      model = 'clustering';
      const modeMap: any = { 'en_ligne': 0, 'magasin': 1, 'mobile': 2 };
      data = [{
        Recency: this.cluster.Recency, Frequency: this.cluster.Frequency,
        Monetary: this.cluster.Monetary, Avg_Basket: this.cluster.Avg_Basket,
        Total_Quantite: this.cluster.Total_Quantite, Nb_Produits: this.cluster.Nb_Produits,
        Total_Remise: this.cluster.Total_Remise, Total_Remboursement: this.cluster.Total_Remboursement,
        Nb_Ventes: this.cluster.Nb_Ventes,
        Mode_Vente_Principal: modeMap[this.cluster.Mode_Vente_Principal] ?? 0,
        Taux_Remise: this.cluster.Taux_Remise, Is_Entreprise: this.cluster.Is_Entreprise,
      }];
    } else {
      model = 'timeseries';
      data = this.tsMonths.map(m => ({
        Date: m.Date, CA_Mensuel: m.CA_Mensuel,
        Quantite_Mensuelle: m.Quantite_Mensuelle, Nb_Commandes: m.Nb_Commandes,
        Remise_Totale: m.Remise_Totale, Prix_Moyen_Unite: m.Prix_Moyen_Unite,
      }));
    }

    this.api.predict(model, data).subscribe({
      next: (res) => {
        this.result = res;
        this.loading = false;
        this.saveToHistory(res);
        this.startResultAnimations();
      },
      error: (err) => { this.error = err.error?.detail || err.message || 'Erreur API inconnue'; this.loading = false; }
    });
  }


  // ── Extraction Facture ──────────────────────────
  onFileSelected(event: any) {
    this.extractionFile = event.target.files[0];
    this.extractionError = "";
    this.extractionSuccess = "";
  }

  extractFacture() {
    if (!this.extractionFile) {
      this.extractionError = "Veuillez sélectionner un fichier PDF.";
      return;
    }
    this.extractionLoading = true;
    this.extractionError = "";
    this.extractionSuccess = "";
    this.api.extractFacture(this.extractionFile).subscribe({
      next: (blob: Blob) => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = (this.extractionFile as any).name.replace(".pdf", ".xlsx");
        a.click();
        window.URL.revokeObjectURL(url);
        this.extractionLoading = false;
        this.extractionSuccess = "Extraction réussie ! Le fichier Excel a été téléchargé.";
      },
      error: (err: any) => {
        this.extractionError = err.error?.detail || "Erreur lors de l\x27extraction.";
        this.extractionLoading = false;
      }
    });
  }
  // ── ETL ─────────────────────────────────────────
  runEtl() {
    this.etlLoading = true;
    this.etlResult = null;
    this.etlError = '';
    this.api.runEtl().subscribe({
      next: (res) => { this.etlResult = res; this.etlLoading = false; },
      error: (err) => { this.etlError = err.error?.detail || 'Erreur ETL'; this.etlLoading = false; }
    });
  }

  // ── Users Management ────────────────────────────
  loadUsers() {
    this.api.getUsers().subscribe({
      next: (res) => { this.usersList = res; },
      error: () => { this.usersList = []; }
    });
  }

  createUser() {
    this.userSuccess = '';
    this.userError = '';
    if (!this.newUser.username || !this.newUser.email || !this.newUser.password) {
      this.userError = 'Veuillez remplir tous les champs.';
      return;
    }
    this.api.adminCreateUser(this.newUser).subscribe({
      next: () => {
        this.userSuccess = 'Compte créé avec succès.';
        this.newUser = { username: '', email: '', password: '', role: 'marketing' };
        this.loadUsers();
      },
      error: (err) => { this.userError = err.error?.detail || 'Erreur lors de la création.'; }
    });
  }

  deleteUser(id: number) {
    if (!confirm('Voulez-vous vraiment désactiver ce compte ?')) return;
    this.api.deleteUser(id).subscribe({
      next: () => { this.loadUsers(); },
      error: (err) => { this.userError = err.error?.detail || 'Erreur.'; }
    });
  }
}