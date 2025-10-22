#!/usr/bin/env python3
"""
Servidor Relay MEJORADO para Railway
Versión más robusta que maneja mejor las conexiones
"""

import socket
import threading
import json
import time
import random
import string
import os
from datetime import datetime

class ImprovedRelayServer:
    def __init__(self):
        # Railway proporciona el puerto automáticamente
        self.port = int(os.environ.get('PORT', 8888))
        self.host = '0.0.0.0'  # Escuchar en todas las interfaces
        
        # Almacenar sesiones activas
        self.sessions = {}  # {session_id: {'server': socket, 'client': socket}}
        self.running = False
        
        print(f"🚀 Relay Server v2.0 iniciando en puerto {self.port}")
        
    def generate_session_id(self):
        """Generar ID de sesión de 6 dígitos"""
        return ''.join(random.choices(string.digits, k=6))
    
    def start(self):
        """Iniciar el servidor relay"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Configurar socket para evitar que se cierre prematuramente
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(50)  # Aumentar backlog
            
            self.running = True
            print(f"✅ Relay Server ACTIVO en {self.host}:{self.port}")
            print("🔗 Esperando conexiones...")
            print("🕐 Tiempo actual:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"📱 Nueva conexión desde {address[0]}:{address[1]} a las {datetime.now().strftime('%H:%M:%S')}")
                    
                    # Configurar socket del cliente
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    client_socket.settimeout(300)  # 5 minutos timeout
                    
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
                        time.sleep(1)  # Evitar loop rápido en caso de error
                        
        except Exception as e:
            print(f"❌ Error crítico iniciando servidor: {e}")
            import traceback
            traceback.print_exc()
    
    def handle_client(self, client_socket, address):
        """Manejar cada cliente conectado"""
        client_id = f"{address[0]}:{address[1]}:{int(time.time())}"
        print(f"🔄 Manejando cliente: {client_id}")
        
        try:
            while self.running:
                try:
                    # Recibir datos del cliente con timeout
                    data = client_socket.recv(4096)
                    if not data:
                        print(f"🔌 Cliente {client_id} envió datos vacíos - desconectando")
                        break
                    
                    print(f"📥 Datos recibidos de {client_id}: {len(data)} bytes")
                    
                    try:
                        # Decodificar mensaje
                        decoded = data.decode('utf-8').strip()
                        print(f"📄 Contenido: {decoded}")
                        
                        # Puede haber múltiples mensajes JSON separados por \n
                        for line in decoded.split('\n'):
                            if line.strip():
                                try:
                                    message = json.loads(line)
                                    print(f"✅ JSON válido procesado: {message}")
                                    self.process_message(client_socket, client_id, message)
                                except json.JSONDecodeError as e:
                                    print(f"⚠️ Error JSON en línea '{line}': {e}")
                                    
                    except UnicodeDecodeError as e:
                        print(f"⚠️ Error decodificando UTF-8 de {client_id}: {e}")
                        continue
                        
                except socket.timeout:
                    print(f"⏰ Timeout con {client_id} - enviando ping")
                    try:
                        ping_msg = {'type': 'ping', 'timestamp': time.time()}
                        self.send_message(client_socket, ping_msg)
                    except:
                        print(f"❌ No se pudo enviar ping a {client_id}")
                        break
                    continue
                    
                except (ConnectionResetError, socket.error) as e:
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
            
            success = self.send_message(sender_socket, response)
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
                self.send_message(self.sessions[session_id]['server'], server_response)
                
                # Confirmar al cliente que se unió
                client_response = {
                    'type': 'session_joined',
                    'session_id': session_id
                }
                self.send_message(sender_socket, client_response)
                
                print(f"🤝 Cliente {sender_id} unido exitosamente a sesión {session_id}")
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
                    print(f"⚠️ Socket desconocido intentando retransmitir en sesión {session_id}")
                    return
                
                if target:
                    relay_message = {
                        'type': 'relayed_data',
                        'data': data,
                        'session_id': session_id
                    }
                    success = self.send_message(target, relay_message)
                    
                    # Solo log para mensajes que no sean screen_update (para evitar spam)
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
            self.send_message(sender_socket, pong)
        
        else:
            print(f"❓ Tipo de mensaje desconocido: {msg_type}")
    
    def send_message(self, socket, message):
        """Enviar mensaje a un socket"""
        try:
            data = json.dumps(message) + '\n'
            bytes_sent = socket.send(data.encode('utf-8'))
            return bytes_sent > 0
        except (socket.error, BrokenPipeError) as e:
            print(f"❌ Error enviando mensaje: {e}")
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
                    self.send_message(session['server'], disconnect_msg)
                if session.get('client') and session['client'] != client_socket:
                    self.send_message(session['client'], disconnect_msg)
                
                del self.sessions[session_id]
                print(f"🗑️ Sesión {session_id} eliminada por desconexión")

if __name__ == "__main__":
    print("🚀 Iniciando Improved Relay Server...")
    relay = ImprovedRelayServer()
    
    try:
        relay.start()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo servidor...")
        relay.running = False
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
