# 🚀 Krypteo - Plateforme de Trading Crypto (Microservices)

Bienvenue sur **Krypteo**, une application de démonstration d'architecture microservices moderne simulant une plateforme de trading de crypto-monnaies.

Ce projet met en œuvre des concepts avancés comme le **Pattern Saga** pour la gestion des transactions distribuées, la communication asynchrone via **Kafka**, et le streaming de données en temps réel.

---

## 🏗️ Architecture du Système

L'application est décomposée en plusieurs services autonomes communiquant principalement via des événements :

-   **Gateway (Port 3010)** : Point d'entrée unique de l'API. Redistribue les requêtes vers les services concernés.
-   **Catalog Service** : Gère l'inventaire des actifs (ex: BTC), les stocks disponibles et récupère les prix en temps réel via l'API Binance (WebSocket).
-   **Order Service** : Gère la création des ordres d'achat/vente et orchestre la Saga de transaction.
-   **Wallet Service** : Gère les soldes des utilisateurs (EUR et Cryptos) et valide les paiements.
-   **Frontend** : Interface utilisateur moderne développée en React/Vite (Recharts pour les graphiques).

### 🛠️ Stack Technique

-   **Backend** : Node.js, Express.js
-   **Frontend** : React, Vite, Recharts, Lucide-React
-   **Base de données** : PostgreSQL (une instance par service pour l'isolation)
-   **Messaging** : Apache Kafka & Zookeeper
-   **Documentation** : Swagger / OpenAPI
-   **Déploiement** : Docker & Docker Compose

---

## 🚦 Pattern Saga

Le projet implémente une **Saga basée sur l'orchestration** pour garantir la cohérence des données lors d'un achat :
1.  `Order Service` crée un ordre en état `PENDING`.
2.  `Catalog Service` réserve le stock de crypto.
3.  `Wallet Service` déduis le montant en EUR.
4.  Si tout réussit, l'ordre passe en `COMPLETED`.
5.  En cas d'échec (solde insuffisant, plus de stock), des **actions de compensation** sont déclenchées pour annuler les réservations précédentes.

---

## 🚀 Démarrage Rapide

### Prérequis
-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)

### Lancement
Pour démarrer l'ensemble de l'écosystème (Base de données, Kafka, Microservices et Frontend), lancez simplement :

```bash
docker-compose up --build
```

Une fois le déploiement terminé, vous pouvez accéder aux services :
-   **Frontend** : [http://localhost:5555](http://localhost:5555)
-   **API Gateway** : [http://localhost:3010](http://localhost:3010)

---

## 📖 Documentation API

Chaque service expose sa propre documentation Swagger. Vous pouvez y accéder via les routes suivantes :
-   **Catalog API** : `http://localhost:3010/catalog/api-docs` (via gateway)
-   **Order API** : `http://localhost:3010/order/api-docs`
-   **Wallet API** : `http://localhost:3010/wallet/api-docs`

---

## 📂 Structure du Projet

```text
krypteo/
├── backend/
│   ├── gateway/         # Proxy & Routage
│   ├── catalog-service/ # Inventaire & Prix temps réel
│   ├── order-service/   # Orchestration des ordres
│   └── wallet-service/  # Gestion des portefeuilles
├── frontend/            # Interface React
└── docker-compose.yml   # Orchestration de l'infrastructure
```

---

## 🛠️ Développement Local

Si vous souhaitez lancer un service individuellement :
1.  Assurez-vous que les infrastructures (Postgres, Kafka) sont lancées via Docker.
2.  Allez dans le dossier du service.
3.  Installez les dépendances : `npm install`.
4.  Configurez les variables d'environnement (`DATABASE_URL`, `KAFKA_BROKERS`).
5.  Lancez le service : `node index.js`.

---

✨ *Projet réalisé dans le cadre du cours d'Architecture Logicielle.*
