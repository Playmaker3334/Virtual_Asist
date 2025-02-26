document.addEventListener('DOMContentLoaded', function() {
    const chatPopup = document.getElementById('chatPopup');
    const openChatButton = document.getElementById('openChat');
    const closeChatButton = document.getElementById('closeChat');
    const chatMessages = document.getElementById('chatMessages');
    const userInput = document.getElementById('userInput');
    const sendButton = document.getElementById('sendButton');

    // Verificar si la librería marked.js está disponible
    if (typeof marked === 'undefined') {
        console.error('Error: marked.js no está cargado. Asegúrate de incluirlo en index.html');
    }

    // Mostrar el chat al hacer clic en el botón flotante
    openChatButton.addEventListener('click', function() {
        chatPopup.style.display = 'flex';
    });

    // Ocultar el chat al hacer clic en la "X"
    closeChatButton.addEventListener('click', function() {
        chatPopup.style.display = 'none';
    });

    // Enviar mensaje al hacer clic en el botón "Enviar"
    sendButton.addEventListener('click', sendMessage);

    // También enviar al presionar Enter
    userInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            sendMessage();
        }
    });

    function sendMessage() {
        const message = userInput.value.trim();
        if (message === '') return;

        // Agregar mensaje del usuario al chat
        addMessage(message, 'user');
        userInput.value = '';

        // Mostrar indicador de carga
        const loadingId = showLoading();

        // Enviar la consulta al servidor
        fetch('/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: message })
        })
        .then(response => response.json())
        .then(data => {
            // Quitar indicador de carga
            hideLoading(loadingId);

            // Agregar respuesta del asistente
            if (data.error) {
                addMessage('❌ **Error:** ' + data.error, 'assistant');
            } else {
                addMessage(data.response, 'assistant');
            }
        })
        .catch(error => {
            hideLoading(loadingId);
            console.error('Error:', error);
            addMessage('❌ **Error de conexión. Inténtalo de nuevo.**', 'assistant');
        });
    }

    // Función para añadir mensajes al contenedor
    function addMessage(content, sender) {
        const messageDiv = document.createElement('div');
        messageDiv.className = 'message ' + sender;

        const messageContent = document.createElement('div');
        messageContent.className = 'message-content';

        // Si marked.js está disponible, renderiza Markdown
        if (typeof marked !== 'undefined') {
            messageContent.innerHTML = marked.parse(content);
        } else {
            messageContent.textContent = content; // Fallback sin Markdown
        }

        messageDiv.appendChild(messageContent);
        chatMessages.appendChild(messageDiv);

        // Desplazar el chat hacia el último mensaje
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    // Funciones para mostrar/ocultar "cargando"
    function showLoading() {
        const loadingDiv = document.createElement('div');
        loadingDiv.className = 'message assistant';
        loadingDiv.id = 'loading-' + Date.now();

        const loadingContent = document.createElement('div');
        loadingContent.className = 'message-content';
        
        const loadingDots = document.createElement('span');
        loadingDots.className = 'loading-dots';
        loadingDots.textContent = '⏳ Pensando...';
        
        loadingContent.appendChild(loadingDots);
        loadingDiv.appendChild(loadingContent);
        chatMessages.appendChild(loadingDiv);
        
        // Desplazar el chat hacia abajo
        chatMessages.scrollTop = chatMessages.scrollHeight;
        
        return loadingDiv.id;
    }

    function hideLoading(loadingId) {
        const loadingElement = document.getElementById(loadingId);
        if (loadingElement) {
            loadingElement.remove();
        }
    }
});


