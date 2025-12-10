import React, { useEffect, useRef, useState } from "react";
import { FileText, User } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

export function ChatMessage({ message }) {
  if (!message) return null;

  const isAssistant = message.role === "assistant";
  const isStreaming = !message.createdAt;

  const [displayedText, setDisplayedText] = useState(message.content);
  const lastContentRef = useRef(message.content);

  // Animación tipo ChatGPT
  useEffect(() => {
    const newText = message.content;
    const previousText = lastContentRef.current;

    if (!newText || newText === previousText) return;

    lastContentRef.current = newText;

    let i = previousText.length;
    const target = newText;

    const step = () => {
      i += 2; // velocidad de tipeo
      if (i >= target.length) {
        setDisplayedText(target);
        return;
      }
      setDisplayedText(target.slice(0, i));
      requestAnimationFrame(step);
    };

    requestAnimationFrame(step);
  }, [message.content]);

  return (
    <div
      className={`flex items-start gap-3 ${
        !isAssistant ? "flex-row-reverse" : ""
      }`}
    >
      {/* Avatar */}
      <div
        className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 ${
          isAssistant ? "bg-blue-600" : "bg-slate-700"
        }`}
      >
        {isAssistant ? (
          <FileText className="h-4 w-4 text-white" />
        ) : (
          <User className="h-4 w-4 text-white" />
        )}
      </div>

      {/* Content */}
      <div className={`flex-1 ${!isAssistant ? "flex justify-end" : ""}`}>
        <div
          className={`rounded-2xl p-4 max-w-[85%] ${
            isAssistant
              ? "bg-slate-100 rounded-tl-sm text-slate-900"
              : "bg-blue-600 rounded-tr-sm text-white"
          }`}
        >
          {/* Markdown + cursor */}
          <div className="prose prose-slate max-w-none whitespace-pre-wrap break-words">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {displayedText}
            </ReactMarkdown>

            {isAssistant && isStreaming && (
              <span className="inline-block w-2 h-4 ml-1 bg-slate-500 animate-pulse rounded-sm"></span>
            )}
          </div>

          {/* Timestamp */}
          {message.createdAt && (
            <div
              className={`text-xs mt-2 ${
                isAssistant ? "text-slate-500" : "text-blue-100"
              }`}
            >
              {new Date(message.createdAt).toLocaleTimeString("es-ES", {
                hour: "2-digit",
                minute: "2-digit",
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// Loading skeleton
function ChatMessageLoading() {
  return (
    <div className="flex items-start gap-3">
      <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 bg-blue-600">
        <FileText className="h-4 w-4 text-white" />
      </div>

      <div className="flex-1">
        <div className="rounded-2xl p-4 max-w-[85%] bg-slate-100 rounded-tl-sm">
          <div className="flex space-x-2 animate-pulse">
            <div className="w-3 h-3 bg-slate-300 rounded-full"></div>
            <div className="w-3 h-3 bg-slate-300 rounded-full"></div>
            <div className="w-3 h-3 bg-slate-300 rounded-full"></div>
          </div>
        </div>
      </div>
    </div>
  );
}

ChatMessage.Loading = ChatMessageLoading;
