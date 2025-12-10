import React from "react";
import { X } from "lucide-react";
import { Button } from "./ui/button";
import { useEffect, useState } from "react";
import { getContractDocument } from "../services/apiService";
import { toast } from "sonner@2.0.3";
export function ContractPreview({ chat, onClose }) {
  const [contractHtml, setContractHtml] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  useEffect(() => {
    const loadContract = async () => {
      if (!chat?.apiChatId) {
        setIsLoading(false);
        return;
      }
      try {
        setIsLoading(true);
        const html = await getContractDocument(chat.apiChatId);
        setContractHtml(html);
      } catch (error) {
        console.error("Error al cargar el contrato:", error);
        toast.error("Error al cargar el documento");
      } finally {
        setIsLoading(false);
      }
    };
    loadContract();
  }, [chat?.apiChatId]);
  return (
    <div className="flex flex-col h-full bg-white">
      {/* HEADER */}
      <div className="border-b border-slate-200 p-4 lg:p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-slate-900">Vista Preliminar del Contrato</h3>
            <p className="text-sm text-slate-500 mt-1">
              • Borrador generado por IA
            </p>
          </div>
          <Button
            variant="ghost"
            size="icon"
            onClick={onClose}
            className="lg:hidden"
          >
            <X className="h-5 w-5" />
          </Button>
        </div>
        {/* ACCIONES DESACTIVADAS */}
        {/* <div className="flex gap-2 opacity-40 pointer-events-none select-none"> <Button variant="outline" size="sm"> Descargar </Button> <Button variant="outline" size="sm"> Imprimir </Button> <Button variant="outline" size="sm"> Compartir </Button> </div> */}
      </div>
      {/* CONTENT */}
      <div className="flex-1 overflow-y-auto">
        <div className="p-6 lg:p-8">
          {isLoading ? (
            <div className="flex items-center justify-center h-64">
              <div className="w-12 h-12 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" />
            </div>
          ) : contractHtml ? (
            <>
              <div
                className="relative bg-white border border-slate-200 shadow-lg rounded-lg p-8 lg:p-12 max-w-4xl mx-auto"
                onContextMenu={(e) => e.preventDefault()}
              >
                {/* WATERMARK EN MALLA – ahora sí por encima del fondo */}
                <div className="watermark-malla"></div>

                {/* CONTENIDO DEL CONTRATO (encima del watermark) */}
                <div
                  className="relative z-30 text-slate-800"
                  dangerouslySetInnerHTML={{ __html: contractHtml }}
                />
              </div>

              {/* DISCLAIMER */}
              <div className="mt-6 p-4 bg-amber-50 border border-amber-200 rounded-lg max-w-4xl mx-auto select-none">
                <p className="text-sm text-amber-800">
                  <strong>Aviso Legal:</strong> Este es un borrador preliminar
                  sin validez jurídica. Será válido únicamente cuando sea
                  formalizado, sellado y firmado.
                </p>
              </div>
            </>
          ) : (
            <div className="flex items-center justify-center h-64">
              <p className="text-slate-500">No hay documento disponible</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
