# TeleResolve — Telecom Customer Complaint Handling System

A full-stack, multi-role customer complaint handling platform for telecom companies with an AI-powered chatbot, automated ticket escalation, and role-based dashboards.

## Architecture

```
backend/          Flask + PostgreSQL + Azure OpenAI
frontend/         React (react-router-dom)
```

**Theme:** White (#FFFFFF) + KPMG Blue (#00338D)
**Chatbot UI:** Preserved exactly from the original `app.py` / `App.jsx` / `App.css`

---

## Features by Role

### Customer
- **Dashboard** — resolved issues, pending tickets, recent sessions
- **Chat Support** — original AI chatbot (unchanged UI), integrated with backend session tracking
  - If issue resolved → session marked resolved, summary saved
  - If not resolved → support ticket auto-created with priority assignment
- **Provide Feedback** — star rating + comments

### Manager
- **Dashboard** — total chats, tickets, critical/high priority counts, avg rating, category breakdown
- **Chat Support** — same AI chatbot for testing/demo
- **Active Tickets** — filterable table (status, priority, search), inline edit status/priority
- **Issue Tracking Table** — all chat sessions with columns: Chat ID (clickable → full chat), User Name/Email, Category, Status, Created At, Resolved At, Resolution Summary

### CTO
- **Executive Overview** — resolution rate %, avg rating, total customers, ticket status breakdown, priority distribution bars, category breakdown, chat system health
- **All Tickets** — same filterable ticket table
- **Issue Tracking** — same tracking table with full chat detail view

---

## Tech Stack

| Layer      | Technology                              |
|------------|-----------------------------------------|
| Frontend   | React 18, React Router 6               |
| Backend    | Flask, Flask-JWT-Extended, Flask-CORS   |
| Database   | PostgreSQL + SQLAlchemy                 |
| AI         | Azure OpenAI GPT-4o-mini               |
| Auth       | JWT (bcrypt password hashing)           |

---

## Setup Instructions

### 1. PostgreSQL Database

```bash
# Create the database
psql -U postgres -c "CREATE DATABASE telecom_complaints;"
```

### 2. Backend

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate          # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your Azure OpenAI credentials and DB URL

# Run the server
python app.py
```

The backend runs on **http://localhost:5500** and auto-creates all tables on first run.

### 3. Frontend

```bash
cd frontend

# Install dependencies
npm install

# Start development server (proxies API to :5500)
npm start
```

The frontend runs on **http://localhost:3000**.

### 4. Environment Variables (backend/.env)

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/telecom_complaints
JWT_SECRET=your-jwt-secret-change-this
SECRET_KEY=your-flask-secret-change-this
AZURE_OPENAI_API_KEY=your-azure-key
AZURE_OPENAI_API_VERSION=2024-02-15-preview
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_DEPLOYMENT_NAME=gpt-4o-mini
```

---

## Database Schema

| Table            | Purpose                                      |
|------------------|----------------------------------------------|
| `users`          | Auth, name, email, role (customer/manager/cto)|
| `chat_sessions`  | Each chatbot interaction session              |
| `chat_messages`  | Individual messages within a session          |
| `tickets`        | Support tickets (auto-created on escalation)  |
| `feedbacks`      | Customer ratings and comments                 |

---

## API Endpoints

### Auth
- `POST /api/auth/register` — Register with role selection
- `POST /api/auth/login` — Login, returns JWT + role-based routing
- `GET /api/auth/me` — Current user info

### Chatbot (original app.py routes — unchanged)
- `GET /api/menu` — Telecom sector menu
- `POST /api/subprocesses` — Subprocesses for a sector
- `POST /api/resolve` — AI resolution generation
- `POST /api/detect-language` — Language detection

### Chat Sessions
- `POST /api/chat/session` — Create new session
- `POST /api/chat/session/:id/message` — Save message
- `PUT /api/chat/session/:id/resolve` — Mark resolved (generates AI summary)
- `PUT /api/chat/session/:id/escalate` — Escalate → auto-creates ticket

### Customer
- `GET /api/customer/dashboard` — Stats + recent sessions

### Manager / CTO
- `GET /api/manager/dashboard` — Full operational stats
- `GET /api/manager/tickets` — Filterable ticket list
- `PUT /api/manager/tickets/:id` — Update ticket status/priority
- `GET /api/manager/chats` — All chat sessions
- `GET /api/cto/overview` — Executive KPIs

### Feedback
- `POST /api/feedback` — Submit feedback
- `GET /api/feedback/list` — List feedbacks

---

## Flow

```
Home Page → Get Started → Login/Register (with role)
  ↓
Customer → Dashboard / Chat Support / Feedback
  Chat: Use chatbot → Issue resolved? 
    YES → Session resolved, summary saved
    NO  → Ticket auto-created → Manager sees it
  ↓
Manager → Dashboard / Chat Support / Active Tickets / Issue Tracking
  Can filter/search tickets, update status/priority, view full chats
  ↓
CTO → Executive Overview / All Tickets / Issue Tracking
  High-level KPIs, resolution rates, priority distribution
```
# tele-cch-kp
# tele-cch-kp
