#!/usr/bin/env python3
"""
Servidor Relay para Asistencia Remota en Railway
Permite que 2 PCs se conecten sin configurar puertos
"""

import socket
import threading
import json
import time
import random
import string
import os
from datetime import datetime

class SimpleRelayServer:
    def __init__(self):
        # Railway proporciona el puerto automáticamente
        self.port = int(os.environ.get('PORT', 8888))
        self.host = '0.0.0.0'  # Escuchar en todas las interfaces
        
        # Almacenar sesiones activas
        self.sessions = {}  # {session_id: {'server': socket, 'client': socket}}
        self.running = False
        
        print(f"Iniciando servidor relay en puerto {self.port}")
        
    def generate_session_id(self):
        """Generar ID de sesión de 6 dígitos"""
        return ''.join(random.choices(string.digits, k=6))
    
    def start(self):
        """Iniciar el servidor relay"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            
            self.running = True
            print(f"✅ Servidor relay activo en {self.host}:{self.port}")
            print("🔗 Esperando conexiones...")
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"📱 Nueva conexión desde {address[0]}")
                    
                    # Crear hilo para manejar cada cliente
                    thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    thread.start()
                    
                except socket.error as e:
                    if self.running:
                        print(f"❌ Error aceptando conexión: {e}")
                        
        except Exception as e:
            print(f"❌ Error iniciando servidor: {e}")
    
    def handle_client(self, client_socket, address):
        """Manejar cada cliente conectado"""
        client_id = f"{address[0]}:{address[1]}"
        print(f"🔄 Manejando cliente: {client_id}")
        
        try:
            while self.running:
                # Recibir datos del cliente
                data = client_socket.recv(4096)
                if not data:
                    print(f"🔌 Cliente {client_id} se desconectó")
                    break
                
                try:
                    message = json.loads(data.decode())
                    self.process_message(client_socket, client_id, message)
                except json.JSONDecodeError as e:
                    print(f"⚠️ Error JSON de {client_id}: {e}")
                    continue
                    
        except (ConnectionResetError, socket.error) as e:
            print(f"🔌 Conexión perdida con {client_id}: {e}")
        finally:
            self.cleanup_client(client_socket)
            client_socket.close()
    
    def process_message(self, sender_socket, sender_id, message):
        """Procesar mensajes de los clientes"""
        msg_type = message.get('type')
        print(f"📨 Mensaje de {sender_id}: {msg_type}")
        
        if msg_type == 'create_session':
            # El servidor crea una nueva sesión
            session_id = self.generate_session_id()
            
            # Verificar que el ID sea único
            while session_id in self.sessions:
                session_id = self.generate_session_id()
            
            self.sessions[session_id] = {
                'server': sender_socket,
                'client': None,
                'created_at': datetime.now(),
                'server_id': sender_id
            }
            
            response = {
                'type': 'session_created',
                'session_id': session_id
            }
            self.send_message(sender_socket, response)
            print(f"🎯 Sesión creada: {session_id} por {sender_id}")
            
        elif msg_type == 'join_session':
            # El cliente se une a una sesión
            session_id = message.get('session_id')
            
            if session_id in self.sessions and self.sessions[session_id]['client'] is None:
                self.sessions[session_id]['client'] = sender_socket
                self.sessions[session_id]['client_id'] = sender_id
                
                # Notificar al servidor que el cliente se conectó
                server_response = {
                    'type': 'client_connected',
                    'session_id': session_id,
                    'client_id': sender_id
                }
                self.send_message(self.sessions[session_id]['server'], server_response)
                
                # Confirmar al cliente que se unió
                client_response = {
                    'type': 'session_joined',
                    'session_id': session_id
                }
                self.send_message(sender_socket, client_response)
                
                print(f"🤝 Cliente {sender_id} se unió a sesión {session_id}")
            else:
                error_response = {
                    'type': 'error',
                    'message': 'Sesión no encontrada o ya ocupada'
                }
                self.send_message(sender_socket, error_response)
                print(f"❌ {sender_id} no pudo unirse a sesión {session_id}")
        
        elif msg_type == 'relay_data':
            # Retransmitir datos entre servidor y cliente
            session_id = message.get('session_id')
            data = message.get('data')
            
            if session_id in self.sessions:
                session = self.sessions[session_id]
                
                # Determinar quién es el destinatario
                if sender_socket == session['server']:
                    target = session['client']
                    target_name = "cliente"
                elif sender_socket == session['client']:
                    target = session['server']
                    target_name = "servidor"
                else:
                    return
                
                if target:
                    relay_message = {
                        'type': 'relayed_data',
                        'data': data
                    }
                    self.send_message(target, relay_message)
                    # Solo imprimir para datos importantes (no cada frame de pantalla)
                    if data.get('type') != 'screen_update':
                        print(f"🔄 Datos retransmitidos a {target_name} en sesión {session_id}")
        
        elif msg_type == 'ping':
            # Mantener conexión viva
            pong = {'type': 'pong'}
            self.send_message(sender_socket, pong)
    
    def send_message(self, socket, message):
        """Enviar mensaje a un socket"""
        try:
            data = json.dumps(message) + '\n'
            socket.send(data.encode())
        except (socket.error, BrokenPipeError) as e:
            print(f"❌ Error enviando mensaje: {e}")
    
    def cleanup_client(self, client_socket):
        """Limpiar cuando un cliente se desconecta"""
        for session_id, session in list(self.sessions.items()):
            if session.get('server') == client_socket or session.get('client') == client_socket:
                # Notificar al otro extremo
                disconnect_msg = {'type': 'session_closed', 'session_id': session_id}
                
                if session.get('server') and session['server'] != client_socket:
                    self.send_message(session['server'], disconnect_msg)
                if session.get('client') and session['client'] != client_socket:
                    self.send_message(session['client'], disconnect_msg)
                
                del self.sessions[session_id]
                print(f"🗑️ Sesión {session_id} eliminada")

if __name__ == "__main__":
    relay = SimpleRelayServer()
    
    try:
        relay.start()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo servidor...")
        relay.running = False
    except Exception as e:
        print(f"❌ Error fatal: {e}")
