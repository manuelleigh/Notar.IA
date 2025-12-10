import React, { useState, useRef, useEffect } from "react";
import { Send, FileText, Loader2 } from "lucide-react";
import { Button } from "./ui/button";
import { ScrollArea } from "./ui/scroll-area";
import { ChatMessage } from "./ChatMessage";
import { isAffirmative } from "../utils/affirmatives";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "./ui/dialog";
import { Input } from "./ui/input";

export function ChatInterface({
  chat,
  onSendMessage,
  onTogglePreview,
  showPreview,
  contexto,
}) {
  const [input, setInput] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [isSignersModalOpen, setIsSignersModalOpen] = useState(false);
  const [signerData, setSignerData] = useState({
    nombre: "",
    correo: "",
    telefono: "",
  });
  const messagesEndRef = useRef(null);
  const viewportRef = useRef(null);
  const inputRef = useRef(null);
  const [shouldAutoScroll, setShouldAutoScroll] = useState(true);

  // Debug: mostrar el estado del contrato
  useEffect(() => {
    if (chat) {
      console.log("ChatInterface - Chat actualizado:", {
        id: chat.id,
        title: chat.title,
        contractGenerated: chat.contractGenerated,
        contratoInfo: chat.contratoInfo,
        messageCount: chat.messages?.length,
        contexto: chat.estado,
      });
    }
  }, [chat, contexto]);

  // Detectar cuando el usuario hace scroll manualmente
  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;

    const handleScroll = () => {
      const { scrollTop, scrollHeight, clientHeight } = viewport;
      const isNearBottom = scrollHeight - scrollTop - clientHeight < 100;
      setShouldAutoScroll(isNearBottom);
    };

    viewport.addEventListener("scroll", handleScroll);
    return () => viewport.removeEventListener("scroll", handleScroll);
  }, []);

  useEffect(() => {
    // Solo hacer auto-scroll si el usuario está cerca del final
    if (shouldAutoScroll) {
      messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [chat?.messages, isSending, shouldAutoScroll]);

  useEffect(() => {
    if (!isSending) {
      inputRef.current?.focus();
    }
  }, [isSending]);

  const handleSignerChange = (e) => {
    const { name, value } = e.target;
    setSignerData((prev) => ({ ...prev, [name]: value }));
  };

  const handleSignerSubmit = () => {
    console.log("Datos del firmante enviados:", signerData);
    setIsSignersModalOpen(false);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const text = input.trim();
    if (!text || isSending) return;

    setShouldAutoScroll(true);
    setInput("");

    if (chat?.estado === "esperando_aprobacion_formal") {
      if (isAffirmative(text)) {
        console.log("Afirmativo detectado → abrir modal");
        setIsSignersModalOpen(true);
        return;
      }
    }

    setIsSending(true);

    try {
      await onSendMessage(text);
    } catch (error) {
      console.error("Error sending message:", error);
    } finally {
      setIsSending(false);
    }

    inputRef.current?.focus();
  };

  if (!chat) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-slate-500">Seleccione o inicie una conversación</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="border-b border-slate-200 p-4 lg:p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-slate-900">{chat.title}</h2>
            <p className="text-sm text-slate-500 mt-1">
              {chat.contractGenerated && "Contrato disponible • "}
              Asistente legal con inteligencia artificial
            </p>
          </div>
          {chat.contractGenerated && (
            <Button
              onClick={onTogglePreview}
              variant={showPreview ? "default" : "outline"}
              className={`hidden lg:flex items-center gap-2 ${
                showPreview
                  ? "bg-blue-600 hover:bg-blue-700 text-white"
                  : "border-blue-600 text-blue-600 hover:bg-blue-50"
              }`}
            >
              <FileText className="h-4 w-4" />
              {showPreview ? "Ocultar" : "Ver"} Contrato
            </Button>
          )}
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-hidden">
        <div ref={viewportRef} className="h-full overflow-y-auto p-4 lg:p-6">
          <div className="max-w-4xl mx-auto space-y-6">
            {chat.messages.map((message) => (
              <ChatMessage key={message.id} message={message} />
            ))}

            {isSending && (
              <div className="flex items-start gap-3">
                <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center flex-shrink-0">
                  <FileText className="h-4 w-4 text-white" />
                </div>
                <div className="flex-1">
                  <div className="bg-slate-100 rounded-2xl rounded-tl-sm p-4 inline-block">
                    <div className="flex gap-1">
                      <div
                        className="w-2 h-2 bg-slate-400 rounded-full animate-bounce"
                        style={{ animationDelay: "0ms" }}
                      />
                      <div
                        className="w-2 h-2 bg-slate-400 rounded-full animate-bounce"
                        style={{ animationDelay: "150ms" }}
                      />
                      <div
                        className="w-2 h-2 bg-slate-400 rounded-full animate-bounce"
                        style={{ animationDelay: "300ms" }}
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        </div>
      </div>

      {/* Input area */}
      <div className="border-t border-slate-200 p-4 lg:p-6 bg-slate-50">
        <form onSubmit={handleSubmit} className="max-w-4xl mx-auto">
          <div className="flex gap-3">
            <input
              ref={inputRef} // Asignar la referencia al campo de entrada
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Describa el contrato que necesita o responda las preguntas..."
              className="flex-1 px-4 py-3 rounded-lg border border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-600 focus:border-transparent bg-white"
              disabled={isSending}
            />
            <Button
              type="submit"
              disabled={!input.trim() || isSending}
              className="bg-blue-600 hover:bg-blue-700 text-white px-6"
            >
              {isSending ? (
                <Loader2 className="h-5 w-5 animate-spin" />
              ) : (
                <Send className="h-5 w-5" />
              )}
            </Button>
          </div>
          <p className="text-xs text-slate-500 mt-2 text-center">
            Los contratos generados deben ser revisados por un profesional legal
          </p>
        </form>
      </div>

      {/* Mobile contract preview button */}
      {chat.contractGenerated && (
        <div className="lg:hidden border-t border-slate-200 p-4">
          <Button
            onClick={onTogglePreview}
            variant="outline"
            className="w-full"
          >
            <FileText className="h-4 w-4 mr-2" />
            Ver Contrato Generado
          </Button>
        </div>
      )}

      {/* Signer data modal */}
      <Dialog open={isSignersModalOpen} onOpenChange={setIsSignersModalOpen}>
        <DialogContent aria-describedby="signers-modal-description">
          <DialogHeader>
            <DialogTitle>Datos del Firmante</DialogTitle>
            <DialogDescription id="signers-modal-description">
              Por favor, complete los datos del firmante para continuar.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <Input
              name="nombre"
              value={signerData.nombre}
              onChange={handleSignerChange}
              placeholder="Nombre completo"
            />
            <Input
              name="correo"
              value={signerData.correo}
              onChange={handleSignerChange}
              placeholder="Correo electrónico"
            />
            <Input
              name="telefono"
              value={signerData.telefono}
              onChange={handleSignerChange}
              placeholder="Teléfono"
            />
          </div>
          <DialogFooter>
            <Button onClick={handleSignerSubmit}>Guardar</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
