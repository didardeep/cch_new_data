import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuth } from './AuthContext';
import HomePage from './pages/HomePage';
import LoginPage from './pages/LoginPage';
import RegisterPage from './pages/RegisterPage';
import CustomerLayout from './pages/customer/CustomerLayout';
import CustomerDashboard from './pages/customer/CustomerDashboard';
import ChatSupport from './pages/customer/ChatSupport';
import FeedbackPage from './pages/customer/FeedbackPage';
import ManagerLayout from './pages/manager/ManagerLayout';
import ManagerDashboard from './pages/manager/ManagerDashboard';
import ManagerChatSupport from './pages/manager/ManagerChatSupport';
import ActiveTickets from './pages/manager/ActiveTickets';
import IssueTracking from './pages/manager/IssueTracking';
import ChatDetail from './pages/manager/ChatDetail';
import CTOLayout from './pages/cto/CTOLayout';
import CTODashboard from './pages/cto/CTODashboard';
import AdminLayout from './pages/admin/AdminLayout';
import AdminDashboard from './pages/admin/AdminDashboard';
import UserManagement from './pages/admin/UserManagement';
import AdminFeedback from './pages/admin/AdminFeedback';
import ReportsPage from './pages/admin/ReportsPage';
import AgentIssues from './pages/admin/AgentIssues';
import AgentLayout from './pages/agent/AgentLayout';
import AgentDashboard from './pages/agent/AgentDashboard';
import AgentTicketBucket from './pages/agent/AgentTicketBucket';
import AgentChatView from './pages/agent/AgentChatView';

function ProtectedRoute({ children, roles }) {
  const { user, loading } = useAuth();
  if (loading) return <div className="page-loader"><div className="spinner" /></div>;
  if (!user) return <Navigate to="/login" />;
  if (roles && !roles.includes(user.role)) return <Navigate to="/login" />;
  return children;
}

export default function App() {
  const { user } = useAuth();

  const getDashboardPath = () => {
    if (!user) return '/login';
    if (user.role === 'customer') return '/customer/dashboard';
    if (user.role === 'manager') return '/manager/dashboard';
    if (user.role === 'cto') return '/cto/dashboard';
    if (user.role === 'admin') return '/admin/dashboard';
    if (user.role === 'human_agent') return '/agent/dashboard';
    return '/login';
  };

  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={user ? <Navigate to={getDashboardPath()} /> : <RegisterPage />} />

      {/* Customer Routes */}
      <Route path="/customer" element={<ProtectedRoute roles={['customer']}><CustomerLayout /></ProtectedRoute>}>
        <Route path="dashboard" element={<CustomerDashboard />} />
        <Route path="chat" element={<ChatSupport />} />
        <Route path="feedback" element={<FeedbackPage />} />
      </Route>

      {/* Manager Routes */}
      <Route path="/manager" element={<ProtectedRoute roles={['manager']}><ManagerLayout /></ProtectedRoute>}>
        <Route path="dashboard" element={<ManagerDashboard />} />
        <Route path="chat" element={<ManagerChatSupport />} />
        <Route path="tickets" element={<ActiveTickets />} />
        <Route path="tracking" element={<IssueTracking />} />
        <Route path="chat-detail/:id" element={<ChatDetail />} />
        <Route path="reports" element={<ReportsPage />} />
      </Route>

      {/* CTO Routes */}
      <Route path="/cto" element={<ProtectedRoute roles={['cto']}><CTOLayout /></ProtectedRoute>}>
        <Route path="dashboard" element={<CTODashboard />} />
        <Route path="tickets" element={<ActiveTickets />} />
        <Route path="tracking" element={<IssueTracking />} />
        <Route path="chat-detail/:id" element={<ChatDetail />} />
      </Route>

      {/* Admin Routes */}
      <Route path="/admin" element={<ProtectedRoute roles={['admin']}><AdminLayout /></ProtectedRoute>}>
        <Route path="dashboard" element={<AdminDashboard />} />
        <Route path="users" element={<UserManagement />} />
        <Route path="tickets" element={<ActiveTickets />} />
        <Route path="tracking" element={<IssueTracking />} />
        <Route path="chat-detail/:id" element={<ChatDetail />} />
        <Route path="agent-issues" element={<AgentIssues />} />
        <Route path="feedback" element={<AdminFeedback />} />
        <Route path="reports" element={<ReportsPage />} />
      </Route>

      {/* Human Agent Routes */}
      <Route path="/agent" element={<ProtectedRoute roles={['human_agent']}><AgentLayout /></ProtectedRoute>}>
        <Route path="dashboard" element={<AgentDashboard />} />
        <Route path="tickets" element={<AgentTicketBucket />} />
        <Route path="chat/:sessionId" element={<AgentChatView />} />
      </Route>

      <Route path="*" element={<Navigate to="/" />} />
    </Routes>
  );
}
