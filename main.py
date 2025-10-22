#!/usr/bin/env python3
"""
Servidor Relay MEJORADO con manejo robusto de imágenes grandes
Soluciona el problema de corrupción en transmisión de pantalla
"""

import socket
import threading
import json
import time
import random
import string
import os
from datetime import datetime

class RobustRelayServer:
    def __init__(self):
        # Railway proporciona el puerto automáticamente
        self.port = int(os.environ.get('PORT', 8888))
        self.host = '0.0.0.0'  # Escuchar en todas las interfaces
        
        # Almacenar sesiones activas
        self.sessions = {}  # {session_id: {'server': socket, 'client': socket}}
        self.running = False
        
        print(f"🚀 Robust Relay Server v3.0 iniciando en puerto {self.port}")
        
    def generate_session_id(self):
        """Generar ID de sesión de 6 dígitos"""
        return ''.join(random.choices(string.digits, k=6))
    
    def start(self):
        """Iniciar el servidor relay"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.AF_INET6)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Configurar socket para manejar datos grandes
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB buffer
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB buffer
            
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(50)
            
            self.running = True
            print(f"✅ Robust Relay Server ACTIVO en {self.host}:{self.port}")
            print("🔗 Esperando conexiones...")
            print("🕐 Tiempo actual:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"📱 Nueva conexión desde {address[0]}:{address[1]} a las {datetime.now().strftime('%H:%M:%S')}")
                    
                    # Configurar socket del cliente para manejar datos grandes
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB buffer
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB buffer
                    client_socket.settimeout(60)  # 1 minuto timeout
                    
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
                        time.sleep(1)
                        
        except Exception as e:
            print(f"❌ Error crítico iniciando servidor: {e}")
            import traceback
            traceback.print_exc()
    
    def handle_client(self, client_socket, address):
        """Manejar cada cliente conectado con buffer robusto"""
        client_id = f"{address[0]}:{address[1]}:{int(time.time())}"
        print(f"🔄 Manejando cliente: {client_id}")
        
        # Buffer para manejar mensajes largos
        message_buffer = b""
        
        try:
            while self.running:
                try:
                    # Recibir datos en chunks más grandes
                    chunk = client_socket.recv(65536)  # 64KB chunks
                    if not chunk:
                        print(f"🔌 Cliente {client_id} envió datos vacíos - desconectando")
                        break
                    
                    message_buffer += chunk
                    
                    # Procesar mensajes completos (terminados en \n)
                    while b'\n' in message_buffer:
                        message_data, message_buffer = message_buffer.split(b'\n', 1)
                        
                        try:
                            # Decodificar el mensaje
                            decoded_message = message_data.decode('utf-8').strip()
                            
                            if decoded_message:  # Solo procesar si no está vacío
                                try:
                                    message = json.loads(decoded_message)
                                    self.process_message(client_socket, client_id, message)
                                except json.JSONDecodeError as e:
                                    # Solo log para mensajes pequeños (evitar spam con imágenes)
                                    if len(decoded_message) < 100:
                                        print(f"⚠️ Error JSON en mensaje corto de {client_id}: {e}")
                                        print(f"   Mensaje: {decoded_message[:50]}...")
                                    # Para mensajes largos (imágenes), intentar recuperar
                                    else:
                                        print(f"⚠️ Mensaje largo corrupto de {client_id} ({len(decoded_message)} chars) - descartando")
                                        
                        except UnicodeDecodeError as e:
                            print(f"⚠️ Error decodificando UTF-8 de {client_id}: {e}")
                            continue
                            
                except socket.timeout:
                    # Enviar ping periódico
                    try:
                        ping_msg = {'type': 'ping', 'timestamp': time.time()}
                        self.send_message_robust(client_socket, ping_msg)
                    except:
                        print(f"❌ No se pudo enviar ping a {client_id}")
                        break
                    continue
                    
                except (ConnectionResetError, socket.error, BrokenPipeError) as e:
                    print(f"🔌 Conexión perdida con {client_id}: {e}")
                    break
                    
        except Exception as e:
            print(f"❌ Error inesperado manejando {client_id}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup_client(client_socket)
            try:
                client_socket.close()
            except:
                pass
            print(f"🗑️ Cliente {client_id} limpiado y desconectado")
    
    def process_message(self, sender_socket, sender_id, message):
        """Procesar mensajes de los clientes"""
        msg_type = message.get('type')
        
        # Solo log para mensajes que no sean screen_update (evitar spam)
        if msg_type != 'relay_data' or message.get('data', {}).get('type') != 'screen_update':
            print(f"📨 Procesando mensaje '{msg_type}' de {sender_id}")
        
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
                'session_id': session_id,
                'timestamp': time.time()
            }
            
            print(f"🎯 Creando sesión {session_id} para {sender_id}")
            
            success = self.send_message_robust(sender_socket, response)
            if success:
                print(f"✅ Respuesta de sesión enviada exitosamente")
            else:
                print(f"❌ Error enviando respuesta de sesión")
            
        elif msg_type == 'join_session':
            # El cliente se une a una sesión
            session_id = message.get('session_id')
            print(f"🔗 Cliente {sender_id} intentando unirse a sesión {session_id}")
            
            if session_id in self.sessions and self.sessions[session_id]['client'] is None:
                self.sessions[session_id]['client'] = sender_socket
                self.sessions[session_id]['client_id'] = sender_id
                
                # Notificar al servidor que el cliente se conectó
                server_response = {
                    'type': 'client_connected',
                    'session_id': session_id,
                    'client_id': sender_id
                }
                self.send_message_robust(self.sessions[session_id]['server'], server_response)
                
                # Confirmar al cliente que se unió
                client_response = {
                    'type': 'session_joined',
                    'session_id': session_id
                }
                self.send_message_robust(sender_socket, client_response)
                
                print(f"🤝 Cliente {sender_id} unido exitosamente a sesión {session_id}")
            else:
                error_response = {
                    'type': 'error',
                    'message': 'Sesión no encontrada o ya ocupada'
                }
                self.send_message_robust(sender_socket, error_response)
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
                    print(f"⚠️ Socket desconocido intentando retransmitir en sesión {session_id}")
                    return
                
                if target:
                    relay_message = {
                        'type': 'relayed_data',
                        'data': data,
                        'session_id': session_id
                    }
                    success = self.send_message_robust(target, relay_message)
                    
                    # Solo log para mensajes que no sean screen_update
                    if data and data.get('type') != 'screen_update':
                        if success:
                            print(f"🔄 Datos retransmitidos a {target_name} en sesión {session_id}")
                        else:
                            print(f"❌ Error retransmitiendo a {target_name} en sesión {session_id}")
                else:
                    print(f"⚠️ No hay {target_name} conectado en sesión {session_id}")
        
        elif msg_type == 'ping':
            # Responder a ping para mantener conexión viva
            pong = {'type': 'pong', 'timestamp': time.time()}
            self.send_message_robust(sender_socket, pong)
        
        else:
            print(f"❓ Tipo de mensaje desconocido: {msg_type}")
    
    def send_message_robust(self, socket, message):
        """Enviar mensaje de forma robusta con manejo de errores mejorado"""
        try:
            data = json.dumps(message, separators=(',', ':')) + '\n'  # JSON compacto
            encoded_data = data.encode('utf-8')
            
            # Enviar en chunks si es muy grande
            total_sent = 0
            while total_sent < len(encoded_data):
                try:
                    sent = socket.send(encoded_data[total_sent:])
                    if sent == 0:
                        print("❌ Socket cerrado durante envío")
                        return False
                    total_sent += sent
                except socket.error as e:
                    print(f"❌ Error enviando chunk: {e}")
                    return False
            
            return True
            
        except (socket.error, BrokenPipeError, ConnectionResetError) as e:
            print(f"❌ Error de socket enviando mensaje: {e}")
            return False
        except Exception as e:
            print(f"❌ Error inesperado enviando mensaje: {e}")
            return False
    
    def cleanup_client(self, client_socket):
        """Limpiar cuando un cliente se desconecta"""
        for session_id, session in list(self.sessions.items()):
            if session.get('server') == client_socket or session.get('client') == client_socket:
                # Notificar al otro extremo
                disconnect_msg = {'type': 'session_closed', 'session_id': session_id}
                
                if session.get('server') and session['server'] != client_socket:
                    self.send_message_robust(session['server'], disconnect_msg)
                if session.get('client') and session['client'] != client_socket:
                    self.send_message_robust(session['client'], disconnect_msg)
                
                del self.sessions[session_id]
                print(f"🗑️ Sesión {session_id} eliminada por desconexión")

if __name__ == "__main__":
    print("🚀 Iniciando Robust Relay Server...")
    relay = RobustRelayServer()
    
    try:
        relay.start()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo servidor...")
        relay.running = False
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
