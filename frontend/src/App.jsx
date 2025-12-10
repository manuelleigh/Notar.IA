import React, { useState, useEffect } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import { ChatInterface } from "./components/ChatInterface";
import { Sidebar } from "./components/Sidebar";
import { ContractPreview } from "./components/ContractPreview";
import { Auth } from "./components/Auth";
import { ForgotPassword } from "./components/ForgotPassword";
import { ResetPassword } from "./components/ResetPassword";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import {
  sendChatMessage,
  getChatHistory,
  getChatDetail,
} from "./services/apiService";
import { toast } from "sonner";
import { Toaster } from "./components/ui/sonner";

// --- Componente de Carga ---
function LoadingScreen() {
  return (
    <div className="flex items-center justify-center h-screen bg-slate-50">
      <div className="text-center">
        <div className="w-16 h-16 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
        <p className="text-slate-600">Cargando...</p>
      </div>
    </div>
  );
}

// --- Componente principal que maneja la autenticación ---
function MainApp() {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return <LoadingScreen />;
  }

  return (
    <Router>
      <Routes>
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route path="/reset-password" element={<ResetPassword />} />
        <Route path="/*" element={isAuthenticated ? <ChatApp /> : <Auth />} />
      </Routes>
    </Router>
  );
}

// --- Aplicación de Chat ---
function ChatApp() {
  const { apiKey } = useAuth();
  const [chats, setChats] = useState([]);
  const [activeChat, setActiveChat] = useState(null);
  const [showContractPreview, setShowContractPreview] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [isLoadingHistory, setIsLoadingHistory] = useState(true);

  const currentChat = chats.find((chat) => chat.id === activeChat);

  useEffect(() => {
    const loadHistory = async () => {
      if (!apiKey) return;
      try {
        setIsLoadingHistory(true);
        const history = await getChatHistory(apiKey);
        const loadedChats = history.map((item) => ({
          id: `chat-${item.chat_id}`,
          title: item.nombre,
          messages: [],
          contractGenerated:
            item.estado === "solo_preliminar" || item.contrato !== null,
          createdAt: new Date(item.fecha_creacion),
          updatedAt: new Date(item.fecha_actualizacion),
          lastMessage: item.ultimo_mensaje,
          messageCount: 0,
          apiChatId: item.chat_id,
          estado: item.estado,
          contratoInfo: item.contrato || undefined,
        }));
        setChats(loadedChats);
      } catch (error) {
        console.error("Error al cargar el historial:", error);
        toast.error("Error al cargar las conversaciones");
      } finally {
        setIsLoadingHistory(false);
      }
    };
    loadHistory();
  }, [apiKey]);

  const handleSelectChat = async (chatId) => {
    setActiveChat(chatId);
    const chat = chats.find((c) => c.id === chatId);
    if (!chat?.apiChatId || !apiKey) {
      setShowContractPreview(false);
      return;
    }
    if (chat.messages.length > 0) {
      setShowContractPreview(chat.contractGenerated);
      return;
    }
    try {
      const detail = await getChatDetail(chat.apiChatId, apiKey);
      const messages = detail.mensajes.map((msg) => ({
        id: `msg-${msg.id}`,
        role: msg.remitente === "usuario" ? "user" : "assistant",
        content: msg.contenido,
        timestamp: new Date(msg.fecha),
      }));
      const hasContract =
        detail.should_show_preview || detail.contrato !== null;
      setChats((prevChats) =>
        prevChats.map((c) =>
          c.id === chatId
            ? {
                ...c,
                messages,
                messageCount: messages.length,
                tipoContrato: detail.chat.metadatos?.tipo_contrato,
                estado: detail.chat.metadatos?.estado, // Asignar el estado aquí
                respuestas: detail.chat.metadatos?.respuestas,
                contratoInfo: detail.contrato
                  ? {
                      id: detail.contrato.id,
                      codigo: detail.contrato.codigo,
                      titulo: detail.contrato.titulo,
                      estado: detail.contrato.estado,
                    }
                  : undefined,
                contractGenerated: hasContract,
              }
            : c
        )
      );
      setShowContractPreview(hasContract);
    } catch (error) {
      console.error("Error al cargar el detalle del chat:", error);
      toast.error("Error al cargar la conversación");
      setShowContractPreview(false);
    }
  };

  const handleNewChat = () => {
    const newChatId = `new-${Date.now()}`;
    const newChat = {
      id: newChatId,
      title: "Nuevo Contrato",
      messages: [
        {
          id: `msg-${Date.now()}`,
          role: "assistant",
          content:
            "Bienvenido al asistente notarial de IA. ¿Qué tipo de contrato necesita generar hoy?",
          timestamp: new Date(),
        },
      ],
      createdAt: new Date(),
      updatedAt: new Date(),
      contractGenerated: false,
    };
    setChats([newChat, ...chats]);
    setActiveChat(newChat.id);
    setShowContractPreview(false);
  };

  const handleSendMessage = async (content) => {
    if (!currentChat || !apiKey) return;
    const userMessage = {
      id: `${Date.now()}-${Math.random()}`,
      role: "user",
      content,
      timestamp: new Date(),
    };
    setChats((prevChats) =>
      prevChats.map((chat) =>
        chat.id === activeChat
          ? {
              ...chat,
              messages: [...chat.messages, userMessage],
              lastMessage: content,
              updatedAt: new Date(),
            }
          : chat
      )
    );
    const isNewChat = currentChat.id.startsWith("new-");
    const titleToSend = isNewChat ? content.slice(0, 50) : undefined;
    const chatIdToSend = isNewChat ? undefined : currentChat.apiChatId;
    try {
      const response = await sendChatMessage(
        content,
        apiKey,
        chatIdToSend,
        titleToSend
      );
      const aiResponse = {
        id: `msg-ai-${Date.now()}`,
        role: "assistant",
        content: response.respuesta,
        timestamp: new Date(),
      };
      const contractIsReady = ["esperando_aprobacion_formal"].includes(
        response.estado
      );
      if (contractIsReady) setShowContractPreview(true);
      setChats((prevChats) =>
        prevChats.map((chat) =>
          chat.id === activeChat
            ? {
                ...chat,
                id: `chat-${response.chat_id}`,
                apiChatId: response.chat_id,
                title: response.nombre || chat.title,
                messages: [...chat.messages, aiResponse],
                lastMessage: response.respuesta,
                estado: response.estado,
                tipoContrato: response.tipo_contrato,
                respuestas: response.respuestas,
                clausulasEspeciales: response.clausulas_especiales,
                contractGenerated: chat.contractGenerated || contractIsReady,
                updatedAt: new Date(),
              }
            : chat
        )
      );
      if (isNewChat) setActiveChat(`chat-${response.chat_id}`);
    } catch (error) {
      console.error("Error al enviar mensaje:", error);
      const errorMessageContent =
        error instanceof Error
          ? error.message
          : "Lo siento, hubo un error al procesar tu mensaje. Por favor, intenta de nuevo.";
      toast.error(errorMessageContent);
      const errorMessage = {
        id: `msg-err-${Date.now()}`,
        role: "assistant",
        content: errorMessageContent,
        timestamp: new Date(),
      };
      setChats((prevChats) =>
        prevChats.map((chat) =>
          chat.id === activeChat
            ? { ...chat, messages: [...chat.messages, errorMessage] }
            : chat
        )
      );
    }
  };

  return (
    <div className="flex h-screen bg-slate-50">
      <Sidebar
        chats={chats}
        activeChat={activeChat || ""}
        onSelectChat={handleSelectChat}
        onNewChat={handleNewChat}
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen(!sidebarOpen)}
        isLoading={isLoadingHistory}
      />
      <div className="flex flex-1 overflow-hidden">
        <div
          className={`flex-1 flex flex-col transition-all duration-300 ease-in-out ${
            showContractPreview && currentChat?.contractGenerated
              ? "lg:w-1/2"
              : "w-full"
          }`}
        >
          <ChatInterface
            chat={currentChat}
            onSendMessage={handleSendMessage}
            onTogglePreview={() => setShowContractPreview(!showContractPreview)}
            showPreview={showContractPreview && currentChat?.contractGenerated}
            contexto={currentChat?.contexto}
          />
        </div>
        {showContractPreview &&
          currentChat?.contractGenerated &&
          currentChat?.apiChatId && (
            <div className="hidden lg:block lg:w-1/2 border-l border-slate-200 transition-all duration-300 ease-in-out">
              <ContractPreview
                chat={currentChat}
                onClose={() => setShowContractPreview(false)}
              />
            </div>
          )}
      </div>
    </div>
  );
}

// --- Proveedor de la aplicación ---
export default function App() {
  return (
    <AuthProvider>
      <MainApp />
      <Toaster />
    </AuthProvider>
  );
}
