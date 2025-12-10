import React from "react";
import { FileText, Plus, Menu, X, LogOut } from "lucide-react";
import { Button } from "./ui/button";
import { useAuth } from "../contexts/AuthContext";

export function Sidebar({
  chats,
  activeChat,
  onSelectChat,
  onNewChat,
  isOpen,
  onToggle,
}) {
  const { logout } = useAuth();

  return (
    <>
      {/* Mobile toggle button */}
      <Button
        variant="ghost"
        size="icon"
        className="lg:hidden fixed top-4 left-4 z-50"
        onClick={onToggle}
      >
        {isOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
      </Button>

      {/* Sidebar */}
      <div
        className={`${
          isOpen ? "translate-x-0" : "-translate-x-full"
        } lg:translate-x-0 fixed lg:static inset-y-0 left-0 z-40 w-80 bg-slate-900 border-r border-slate-800 flex flex-col transition-transform duration-200 ease-in-out`}
      >
        {/* Header */}
        <div className="p-6 border-b border-slate-800">
          <div className="flex items-center gap-3 mb-6">
            <div className="p-2 bg-blue-600 rounded-lg">
              <FileText className="h-6 w-6 text-white" />
            </div>
            <div>
              <h1 className="text-white">Asistente Notarial</h1>
              <p className="text-slate-400 text-sm">
                Generación de Contratos IA
              </p>
            </div>
          </div>

          <Button
            onClick={onNewChat}
            className="w-full bg-blue-600 hover:bg-blue-700 text-white"
          >
            <Plus className="h-4 w-4 mr-2" />
            Nuevo Contrato
          </Button>
        </div>

        {/* Chat list */}
        <div className="flex-1 overflow-y-auto p-4 sidebar-scroll">
          <div className="space-y-2">
            <div className="text-slate-400 text-xs uppercase tracking-wider px-2 mb-2">
              Conversaciones Recientes
            </div>
            {chats.length === 0 ? (
              <div className="text-center text-slate-500 py-8 text-sm">
                No hay conversaciones aún.
                <br />
                Crea una nueva para comenzar.
              </div>
            ) : (
              chats.map((chat) => (
                <button
                  key={chat.id}
                  onClick={() => onSelectChat(chat.id)}
                  className={`w-full text-left p-3 rounded-lg transition-colors ${
                    activeChat === chat.id
                      ? "bg-slate-800 text-white"
                      : "text-slate-300 hover:bg-slate-800/50"
                  }`}
                >
                  <div className="flex items-start gap-2">
                    <FileText
                      className={`h-4 w-4 mt-0.5 flex-shrink-0 ${
                        chat.contractGenerated
                          ? "text-green-500"
                          : "text-slate-400"
                      }`}
                    />
                    <div className="flex-1 min-w-0">
                      <p className="truncate text-sm">{chat.title}</p>
                      {chat.lastMessage && (
                        <p className="text-xs text-slate-500 truncate mt-0.5">
                          {chat.lastMessage}
                        </p>
                      )}
                      <p className="text-xs text-slate-600 mt-1">
                        {chat.messageCount
                          ? `${chat.messageCount} mensajes`
                          : "Sin mensajes"}
                        {chat.contractGenerated && " • Contrato generado"}
                      </p>
                    </div>
                  </div>
                </button>
              ))
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-slate-800 space-y-3">
          <Button
            onClick={logout}
            variant="ghost"
            className="w-full text-slate-400 hover:text-white hover:bg-slate-800"
          >
            <LogOut className="h-4 w-4 mr-2" />
            Cerrar Sesión
          </Button>
          <div className="text-xs text-slate-500 text-center">
            <p>Asistente legal con IA</p>
            <p className="mt-1">© 2025 NotarIA</p>
          </div>
        </div>
      </div>

      {/* Overlay for mobile */}
      {isOpen && (
        <div
          className="lg:hidden fixed inset-0 bg-black/50 z-30"
          onClick={onToggle}
        />
      )}
    </>
  );
}
