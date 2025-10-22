#!/usr/bin/env python3
"""
🚀 SERVIDOR RELAY 10/10 - VERSIÓN ULTIMATE
Optimizado para máximo rendimiento en escritorio remoto
"""

import asyncio
import json
import time
import random
import string
import os
import logging
import signal
import sys
import zlib
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Any
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor

# ======================== CONFIGURACIÓN PROFESIONAL ========================
@dataclass
class RelayConfig:
    """Configuración centralizada del servidor"""
    port: int = int(os.environ.get('PORT', 8888))
    host: str = os.environ.get('HOST', '0.0.0.0')
    max_connections: int = int(os.environ.get('MAX_CONNECTIONS', 100))
    target_fps: int = int(os.environ.get('TARGET_FPS', 30))
    frame_buffer_size: int = int(os.environ.get('FRAME_BUFFER_SIZE', 3))
    compression_level: int = int(os.environ.get('COMPRESSION_LEVEL', 6))
    log_level: str = os.environ.get('LOG_LEVEL', 'INFO')
    max_message_size: int = int(os.environ.get('MAX_MESSAGE_SIZE', 10 * 1024 * 1024))  # 10MB
    heartbeat_interval: int = int(os.environ.get('HEARTBEAT_INTERVAL', 30))
    session_timeout: int = int(os.environ.get('SESSION_TIMEOUT', 300))  # 5 minutos

# ======================== FRAME BUFFER INTELIGENTE ========================
class IntelligentFrameBuffer:
    """Buffer que mantiene solo los frames más recientes y descarta antiguos"""
    
    def __init__(self, max_size: int = 3):
        self.max_size = max_size
        self.frames = deque(maxlen=max_size)
        self.lock = asyncio.Lock()
        self.last_frame_time = 0
        self.frame_count = 0
        
    async def add_frame(self, frame_data: Any) -> bool:
        """Agregar frame, descartando automáticamente los viejos"""
        async with self.lock:
            current_time = time.time()
            
            # Rate limiting - máximo 60 FPS
            if current_time - self.last_frame_time < 1.0/60:
                return False  # Descarta frame por rate limiting
            
            # Agregar frame (automáticamente descarta el más viejo si está lleno)
            self.frames.append({
                'data': frame_data,
                'timestamp': current_time,
                'frame_id': self.frame_count
            })
            
            self.last_frame_time = current_time
            self.frame_count += 1
            return True
    
    async def get_latest_frame(self) -> Optional[Any]:
        """Obtener el frame más reciente y limpiar buffer"""
        async with self.lock:
            if not self.frames:
                return None
            
            # Obtener el más reciente
            latest = self.frames[-1]
            
            # Limpiar frames antiguos (mantener solo el último)
            self.frames.clear()
            self.frames.append(latest)
            
            return latest['data']
    
    async def get_stats(self) -> Dict[str, Any]:
        """Estadísticas del buffer"""
        async with self.lock:
            return {
                'buffer_size': len(self.frames),
                'total_frames': self.frame_count,
                'fps': self.frame_count / max(time.time() - (self.frames[0]['timestamp'] if self.frames else time.time()), 1)
            }

# ======================== COMPRESIÓN AVANZADA ========================
class AdvancedCompressor:
    """Sistema de compresión optimizado para imágenes y datos"""
    
    @staticmethod
    def compress_data(data: bytes, level: int = 6) -> bytes:
        """Comprimir datos con zlib optimizado"""
        try:
            return zlib.compress(data, level)
        except Exception:
            return data  # Fallback sin compresión
    
    @staticmethod
    def decompress_data(data: bytes) -> bytes:
        """Descomprimir datos"""
        try:
            return zlib.decompress(data)
        except Exception:
            return data  # Fallback sin descompresión
    
    @staticmethod
    def compress_json(obj: Any, compression_level: int = 6) -> bytes:
        """Comprimir objeto JSON"""
        json_str = json.dumps(obj, separators=(',', ':'))
        json_bytes = json_str.encode('utf-8')
        
        # Solo comprimir si vale la pena (>1KB)
        if len(json_bytes) > 1024:
            compressed = AdvancedCompressor.compress_data(json_bytes, compression_level)
            if len(compressed) < len(json_bytes) * 0.8:  # Solo si reduce al menos 20%
                return b'COMPRESSED:' + compressed
        
        return json_bytes

# ======================== MÉTRICAS Y MONITOREO ========================
class ServerMetrics:
    """Sistema de métricas para monitoreo en tiempo real"""
    
    def __init__(self):
        self.start_time = time.time()
        self.total_connections = 0
        self.active_connections = 0
        self.total_messages = 0
        self.total_bytes_sent = 0
        self.total_bytes_received = 0
        self.session_count = 0
        self.frame_stats = {'total': 0, 'dropped': 0, 'compressed': 0}
        self.error_count = 0
        
        # Estadísticas por minuto
        self.minute_stats = deque(maxlen=60)
        self.last_minute_update = time.time()
        
    def record_connection(self):
        self.total_connections += 1
        self.active_connections += 1
    
    def record_disconnection(self):
        self.active_connections = max(0, self.active_connections - 1)
    
    def record_message(self, size_bytes: int = 0):
        self.total_messages += 1
        self.total_bytes_received += size_bytes
        
    def record_sent(self, size_bytes: int = 0):
        self.total_bytes_sent += size_bytes
    
    def record_frame(self, compressed: bool = False, dropped: bool = False):
        self.frame_stats['total'] += 1
        if compressed:
            self.frame_stats['compressed'] += 1
        if dropped:
            self.frame_stats['dropped'] += 1
    
    def record_error(self):
        self.error_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        uptime = time.time() - self.start_time
        return {
            'uptime_seconds': uptime,
            'active_connections': self.active_connections,
            'total_connections': self.total_connections,
            'total_messages': self.total_messages,
            'bytes_sent': self.total_bytes_sent,
            'bytes_received': self.total_bytes_received,
            'session_count': self.session_count,
            'frame_stats': self.frame_stats,
            'error_count': self.error_count,
            'messages_per_second': self.total_messages / max(uptime, 1),
            'avg_connection_time': uptime / max(self.total_connections, 1)
        }

# ======================== SESIÓN AVANZADA ========================
@dataclass
class AdvancedSession:
    """Sesión con capacidades avanzadas"""
    session_id: str
    created_at: datetime
    server_writer: Optional[asyncio.StreamWriter] = None
    client_writer: Optional[asyncio.StreamWriter] = None
    server_id: str = ""
    client_id: str = ""
    frame_buffer: IntelligentFrameBuffer = field(default_factory=lambda: IntelligentFrameBuffer())
    last_activity: datetime = field(default_factory=datetime.now)
    total_messages: int = 0
    compression_enabled: bool = True
    
    def is_complete(self) -> bool:
        return self.server_writer is not None and self.client_writer is not None
    
    def is_expired(self, timeout_seconds: int = 300) -> bool:
        return (datetime.now() - self.last_activity).total_seconds() > timeout_seconds
    
    def update_activity(self):
        self.last_activity = datetime.now()
        self.total_messages += 1

# ======================== SERVIDOR RELAY ULTIMATE ========================
class UltimateRelayServer:
    """Servidor relay optimizado para máximo rendimiento"""
    
    def __init__(self, config: RelayConfig):
        self.config = config
        self.sessions: Dict[str, AdvancedSession] = {}
        self.metrics = ServerMetrics()
        self.running = False
        self.server = None
        
        # Thread pool para operaciones blocking
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Configurar logging
        self.setup_logging()
        
        # Configurar graceful shutdown
        self.setup_signal_handlers()
        
        self.logger.info("🚀 Ultimate Relay Server iniciado", extra={
            'config': {
                'port': config.port,
                'max_connections': config.max_connections,
                'target_fps': config.target_fps
            }
        })
    
    def setup_logging(self):
        """Configurar logging estructurado"""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('relay_ultimate.log', mode='a')
            ]
        )
        self.logger = logging.getLogger('UltimateRelay')
    
    def setup_signal_handlers(self):
        """Configurar manejo de señales para graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"📢 Recibida señal {signum}, cerrando gracefully...")
            asyncio.create_task(self.graceful_shutdown())
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    def generate_session_id(self) -> str:
        """Generar ID único de sesión"""
        while True:
            session_id = ''.join(random.choices(string.digits, k=6))
            if session_id not in self.sessions:
                return session_id
    
    async def start(self):
        """Iniciar servidor con asyncio"""
        try:
            self.server = await asyncio.start_server(
                self.handle_client,
                self.config.host,
                self.config.port,
                limit=self.config.max_message_size
            )
            
            self.running = True
            
            # Iniciar tareas de mantenimiento
            asyncio.create_task(self.cleanup_task())
            asyncio.create_task(self.heartbeat_task())
            asyncio.create_task(self.metrics_task())
            
            self.logger.info(f"✅ Servidor activo en {self.config.host}:{self.config.port}")
            self.logger.info(f"📊 Configuración: FPS={self.config.target_fps}, Buffers={self.config.frame_buffer_size}")
            
            async with self.server:
                await self.server.serve_forever()
                
        except Exception as e:
            self.logger.error(f"❌ Error crítico: {e}", exc_info=True)
            await self.graceful_shutdown()
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Manejar cliente con asyncio optimizado"""
        client_addr = writer.get_extra_info('peername')
        client_id = f"{client_addr[0]}:{client_addr[1]}:{int(time.time())}"
        
        self.metrics.record_connection()
        self.logger.info(f"📱 Nueva conexión: {client_id}")
        
        buffer = b""
        
        try:
            while self.running:
                try:
                    # Leer con timeout
                    chunk = await asyncio.wait_for(reader.read(65536), timeout=30.0)
                    if not chunk:
                        break
                    
                    buffer += chunk
                    
                    # Procesar mensajes completos
                    while b'\n' in buffer:
                        message_data, buffer = buffer.split(b'\n', 1)
                        
                        if message_data:
                            await self.process_message_ultimate(
                                message_data, writer, client_id
                            )
                            
                except asyncio.TimeoutError:
                    # Enviar heartbeat
                    await self.send_message_ultimate(writer, {
                        'type': 'heartbeat',
                        'timestamp': time.time()
                    })
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Error manejando {client_id}: {e}")
            self.metrics.record_error()
        finally:
            await self.cleanup_client(writer)
            self.metrics.record_disconnection()
            self.logger.info(f"🔌 Cliente desconectado: {client_id}")
    
    async def process_message_ultimate(self, message_data: bytes, writer: asyncio.StreamWriter, client_id: str):
        """Procesar mensaje con optimizaciones avanzadas"""
        try:
            # Intentar descomprimir si es necesario
            if message_data.startswith(b'COMPRESSED:'):
                message_data = AdvancedCompressor.decompress_data(message_data[11:])
            
            # Decodificar JSON
            message_str = message_data.decode('utf-8')
            message = json.loads(message_str)
            
            self.metrics.record_message(len(message_data))
            
            msg_type = message.get('type')
            
            # Log solo para mensajes importantes (no screen_update)
            if msg_type not in ['relay_data', 'heartbeat']:
                self.logger.info(f"📨 {msg_type} de {client_id}")
            
            # Procesar según tipo
            if msg_type == 'create_session':
                await self.handle_create_session(writer, client_id)
                
            elif msg_type == 'join_session':
                await self.handle_join_session(message, writer, client_id)
                
            elif msg_type == 'relay_data':
                await self.handle_relay_data_ultimate(message, writer)
                
            elif msg_type == 'heartbeat':
                await self.send_message_ultimate(writer, {
                    'type': 'heartbeat_response',
                    'timestamp': time.time()
                })
                
        except Exception as e:
            self.logger.error(f"❌ Error procesando mensaje de {client_id}: {e}")
            self.metrics.record_error()
    
    async def handle_create_session(self, writer: asyncio.StreamWriter, client_id: str):
        """Crear nueva sesión optimizada"""
        session_id = self.generate_session_id()
        
        session = AdvancedSession(
            session_id=session_id,
            created_at=datetime.now(),
            server_writer=writer,
            server_id=client_id
        )
        
        self.sessions[session_id] = session
        self.metrics.session_count += 1
        
        response = {
            'type': 'session_created',
            'session_id': session_id,
            'timestamp': time.time(),
            'server_config': {
                'target_fps': self.config.target_fps,
                'compression_enabled': True
            }
        }
        
        await self.send_message_ultimate(writer, response)
        self.logger.info(f"🎯 Sesión {session_id} creada para {client_id}")
    
    async def handle_join_session(self, message: Dict, writer: asyncio.StreamWriter, client_id: str):
        """Unirse a sesión con validación"""
        session_id = message.get('session_id')
        
        if session_id in self.sessions:
            session = self.sessions[session_id]
            
            if session.client_writer is None:
                session.client_writer = writer
                session.client_id = client_id
                session.update_activity()
                
                # Notificar al servidor
                if session.server_writer:
                    await self.send_message_ultimate(session.server_writer, {
                        'type': 'client_connected',
                        'session_id': session_id,
                        'client_id': client_id
                    })
                
                # Confirmar al cliente
                await self.send_message_ultimate(writer, {
                    'type': 'session_joined',
                    'session_id': session_id,
                    'server_config': {
                        'target_fps': self.config.target_fps,
                        'compression_enabled': session.compression_enabled
                    }
                })
                
                self.logger.info(f"🤝 Cliente {client_id} unido a sesión {session_id}")
            else:
                await self.send_message_ultimate(writer, {
                    'type': 'error',
                    'message': 'Sesión ya ocupada'
                })
        else:
            await self.send_message_ultimate(writer, {
                'type': 'error',
                'message': 'Sesión no encontrada'
            })
    
    async def handle_relay_data_ultimate(self, message: Dict, sender_writer: asyncio.StreamWriter):
        """Retransmisión de datos con frame buffer inteligente"""
        session_id = message.get('session_id')
        data = message.get('data', {})
        
        if session_id not in self.sessions:
            return
        
        session = self.sessions[session_id]
        session.update_activity()
        
        # Determinar destinatario
        if sender_writer == session.server_writer:
            target_writer = session.client_writer
            target_name = "cliente"
        elif sender_writer == session.client_writer:
            target_writer = session.server_writer
            target_name = "servidor"
        else:
            return
        
        if not target_writer:
            return
        
        # Manejo especial para screen_update (usar frame buffer)
        if data.get('type') == 'screen_update':
            # Agregar al frame buffer (puede ser descartado si hay muchos)
            frame_added = await session.frame_buffer.add_frame(data)
            
            if frame_added:
                # Obtener el frame más reciente del buffer
                latest_frame = await session.frame_buffer.get_latest_frame()
                if latest_frame:
                    relay_message = {
                        'type': 'relayed_data',
                        'data': latest_frame,
                        'session_id': session_id
                    }
                    await self.send_message_ultimate(target_writer, relay_message)
                    self.metrics.record_frame(compressed=session.compression_enabled)
                else:
                    self.metrics.record_frame(dropped=True)
            else:
                self.metrics.record_frame(dropped=True)
        else:
            # Para otros tipos de mensaje, envío directo
            relay_message = {
                'type': 'relayed_data',
                'data': data,
                'session_id': session_id
            }
            await self.send_message_ultimate(target_writer, relay_message)
    
    async def send_message_ultimate(self, writer: asyncio.StreamWriter, message: Dict) -> bool:
        """Envío optimizado con compresión"""
        try:
            # Comprimir mensaje si es grande
            data = AdvancedCompressor.compress_json(message, self.config.compression_level)
            data += b'\n'
            
            writer.write(data)
            await writer.drain()
            
            self.metrics.record_sent(len(data))
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando mensaje: {e}")
            self.metrics.record_error()
            return False
    
    async def cleanup_client(self, writer: asyncio.StreamWriter):
        """Limpiar cliente desconectado"""
        for session_id, session in list(self.sessions.items()):
            if session.server_writer == writer or session.client_writer == writer:
                # Notificar al otro extremo
                disconnect_msg = {
                    'type': 'session_closed',
                    'session_id': session_id,
                    'reason': 'peer_disconnected'
                }
                
                if session.server_writer and session.server_writer != writer:
                    await self.send_message_ultimate(session.server_writer, disconnect_msg)
                if session.client_writer and session.client_writer != writer:
                    await self.send_message_ultimate(session.client_writer, disconnect_msg)
                
                del self.sessions[session_id]
                self.logger.info(f"🗑️ Sesión {session_id} eliminada")
    
    async def cleanup_task(self):
        """Tarea de limpieza periódica"""
        while self.running:
            try:
                # Limpiar sesiones expiradas
                expired_sessions = [
                    sid for sid, session in self.sessions.items()
                    if session.is_expired(self.config.session_timeout)
                ]
                
                for session_id in expired_sessions:
                    session = self.sessions[session_id]
                    self.logger.info(f"⏰ Sesión {session_id} expirada, eliminando")
                    
                    # Notificar a los clientes
                    disconnect_msg = {
                        'type': 'session_closed',
                        'session_id': session_id,
                        'reason': 'timeout'
                    }
                    
                    if session.server_writer:
                        await self.send_message_ultimate(session.server_writer, disconnect_msg)
                    if session.client_writer:
                        await self.send_message_ultimate(session.client_writer, disconnect_msg)
                    
                    del self.sessions[session_id]
                
                await asyncio.sleep(60)  # Limpiar cada minuto
                
            except Exception as e:
                self.logger.error(f"❌ Error en tarea de limpieza: {e}")
                await asyncio.sleep(10)
    
    async def heartbeat_task(self):
        """Tarea de heartbeat para mantener conexiones vivas"""
        while self.running:
            try:
                heartbeat_msg = {
                    'type': 'server_heartbeat',
                    'timestamp': time.time(),
                    'active_sessions': len(self.sessions)
                }
                
                # Enviar heartbeat a todas las sesiones activas
                for session in self.sessions.values():
                    if session.server_writer:
                        await self.send_message_ultimate(session.server_writer, heartbeat_msg)
                    if session.client_writer:
                        await self.send_message_ultimate(session.client_writer, heartbeat_msg)
                
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except Exception as e:
                self.logger.error(f"❌ Error en heartbeat: {e}")
                await asyncio.sleep(5)
    
    async def metrics_task(self):
        """Tarea de métricas y logging de estadísticas"""
        while self.running:
            try:
                stats = self.metrics.get_stats()
                
                # Log estadísticas cada 5 minutos
                self.logger.info(f"📊 Stats: {stats['active_connections']} conexiones, "
                               f"{stats['messages_per_second']:.1f} msg/s, "
                               f"{stats['frame_stats']['total']} frames "
                               f"({stats['frame_stats']['dropped']} descartados)")
                
                await asyncio.sleep(300)  # Cada 5 minutos
                
            except Exception as e:
                self.logger.error(f"❌ Error en métricas: {e}")
                await asyncio.sleep(60)
    
    async def graceful_shutdown(self):
        """Cierre graceful del servidor"""
        self.logger.info("🛑 Iniciando cierre graceful...")
        self.running = False
        
        # Notificar a todos los clientes
        disconnect_msg = {
            'type': 'server_shutdown',
            'message': 'Servidor reiniciándose',
            'timestamp': time.time()
        }
        
        for session in self.sessions.values():
            if session.server_writer:
                await self.send_message_ultimate(session.server_writer, disconnect_msg)
            if session.client_writer:
                await self.send_message_ultimate(session.client_writer, disconnect_msg)
        
        # Cerrar servidor
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Estadísticas finales
        final_stats = self.metrics.get_stats()
        self.logger.info(f"📈 Estadísticas finales: {final_stats}")
        
        self.logger.info("✅ Servidor cerrado gracefully")

# ======================== PUNTO DE ENTRADA ========================
async def main():
    """Función principal"""
    print("🚀 Iniciando Ultimate Relay Server...")
    
    config = RelayConfig()
    server = UltimateRelayServer(config)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        print("\n🛑 Interrupción por teclado")
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await server.graceful_shutdown()

if __name__ == "__main__":
    # Configurar asyncio para mejor rendimiento
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Terminado por usuario")
