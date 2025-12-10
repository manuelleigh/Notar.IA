const API_BASE_URL = "http://127.0.0.1:5000";

export async function getChatHistory(apiKey) {
  const response = await fetch(`${API_BASE_URL}/chat/historial`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
  });

  if (!response.ok) {
    if (response.status === 401) {
      throw new Error("Token requerido");
    }
    throw new Error("Error al obtener el historial");
  }

  const result = await response.json();
  // console.log("Respuesta de la API para getChatHistory:", result); // Depuración
  return result.data; // Devolver solo el array de chats
}

export async function getChatDetail(chatId, apiKey) {
  const response = await fetch(`${API_BASE_URL}/chat/${chatId}`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
  });

  if (!response.ok) {
    if (response.status === 401) {
      throw new Error("Token requerido");
    }
    if (response.status === 404) {
      throw new Error("Chat no encontrado");
    }
    throw new Error("Error al obtener el chat");
  }

  const data = await response.json();
  // console.log("Respuesta de la API para getChatDetail:", data); // Depuración
  return data;
}

export async function sendChatMessage(mensaje, apiKey, chatId, nombre) {
  const body = { mensaje };
  if (chatId) {
    body.chat_id = chatId;
  }
  if (nombre) {
    body.nombre = nombre;
  }

  const response = await fetch(`${API_BASE_URL}/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    if (response.status === 401) {
      throw new Error("Token requerido");
    }
    if (response.status === 403) {
      throw new Error("Usuario no autorizado");
    }
    if (response.status === 404) {
      throw new Error("Chat no encontrado");
    }
    throw new Error("Error al enviar mensaje");
  }

  return response.json();
}

export async function getContractDocument(chatId) {
  const response = await fetch(
    `${API_BASE_URL}/chat/documento?chat_id=${chatId}`,
    {
      method: "GET",
    }
  );

  if (!response.ok) {
    if (response.status === 404) {
      throw new Error("Chat no encontrado");
    }
    throw new Error("Error al obtener el documento");
  }

  return response.text();
}
