import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set
import websockets


# ==========================================
# 1. СТАНДАРТНЫЕ БЛОКИ МЭК 61131-3
# ==========================================

class R_TRIG:
    """Детектор переднего фронта (переход 0 -> 1)"""
    def __init__(self):
        self._prev_clk: bool = False
        self.q: bool = False

    def __call__(self, clk: bool) -> bool:
        self.q = bool(clk) and not self._prev_clk
        self._prev_clk = bool(clk)
        return self.q


class F_TRIG:
    """Детектор заднего фронта (переход 1 -> 0)"""
    def __init__(self):
        self._prev_clk: bool = False
        self.q: bool = False

    def __call__(self, clk: bool) -> bool:
        self.q = not bool(clk) and self._prev_clk
        self._prev_clk = bool(clk)
        return self.q


class TON:
    """Таймер задержки включения (Timer On-Delay)"""
    def __init__(self, pt: float):
        """
        :param pt: Preset Time — уставка времени в секундах (например, 1.5)
        """
        self.pt: float = pt
        self.et: float = 0.0  # Elapsed Time — прошедшее время
        self.q: bool = False  # Выход таймера
        self._start_time: Optional[float] = None

    def __call__(self, in_sig: bool, pt: Optional[float] = None) -> bool:
        if pt is not None:
            self.pt = pt

        if not in_sig:
            self._start_time = None
            self.et = 0.0
            self.q = False
        else:
            now = time.monotonic()
            if self._start_time is None:
                self._start_time = now
            self.et = round(now - self._start_time, 3)
            if self.et >= self.pt:
                self.et = self.pt
                self.q = True
            else:
                self.q = False
        return self.q

    def reset(self):
        self._start_time = None
        self.et = 0.0
        self.q = False


class TOF:
    """Таймер задержки выключения (Timer Off-Delay)"""
    def __init__(self, pt: float):
        self.pt: float = pt
        self.et: float = 0.0
        self.q: bool = False
        self._stop_time: Optional[float] = None
        self._was_on: bool = False

    def __call__(self, in_sig: bool, pt: Optional[float] = None) -> bool:
        if pt is not None:
            self.pt = pt

        if in_sig:
            self._was_on = True
            self._stop_time = None
            self.et = 0.0
            self.q = True
        else:
            if not self._was_on:
                self.q = False
                self.et = 0.0
            else:
                now = time.monotonic()
                if self._stop_time is None:
                    self._stop_time = now
                self.et = round(now - self._stop_time, 3)
                if self.et >= self.pt:
                    self.et = self.pt
                    self.q = False
                    self._was_on = False
                else:
                    self.q = True
        return self.q


# ==========================================
# 2. КОНЕЧНЫЙ АВТОМАТ (STATE MACHINE)
# ==========================================

class StateMachine:
    """Базовый класс пошагового технологического цикла"""
    def __init__(self, initial_state: int = 0, state_names: Optional[Dict[int, str]] = None):
        self.state: int = initial_state
        self.prev_state: int = initial_state
        self.state_names: Dict[int, str] = state_names or {}
        self.state_timer = TON(0.0)
        self._entry_flag: bool = True

    @property
    def current_name(self) -> str:
        return self.state_names.get(self.state, f"STEP_{self.state}")

    @property
    def time_in_state(self) -> float:
        """Сколько секунд автомат находится в текущем шаге"""
        return self.state_timer.et

    def is_entry(self) -> bool:
        """Возвращает True только в самом первом скане после перехода в новый шаг"""
        if self._entry_flag:
            self._entry_flag = False
            return True
        return False

    def transition_to(self, new_state: int):
        """Безопасный переход на следующий шаг"""
        if new_state != self.state:
            self.prev_state = self.state
            self.state = new_state
            self.state_timer.reset()
            self._entry_flag = True

    def update(self):
        """Вызывается в каждом цикле сканирования для обновления внутреннего таймера шага"""
        self.state_timer(True, pt=999999.0)


# ==========================================
# 3. ЯДРО ПЛК И ЦИКЛ СКАНИРОВАНИЯ
# ==========================================

@dataclass
class PLCDiagnostics:
    scan_count: int = 0
    actual_scan_ms: float = 0.0
    max_scan_ms: float = 0.0
    overruns: int = 0  # Сколько раз логика выполнялась дольше заданного цикла


class PLCCore(ABC):
    """
    Базовый класс контроллера.
    Наследуется в конкретном проекте (например, DrillingMachinePLC).
    """
    def __init__(self, scan_time_ms: int = 50, hmi_ws_port: int = 8765):
        self.scan_time_sec: float = scan_time_ms / 1000.0
        self.hmi_ws_port: int = hmi_ws_port

        # Образ процесса (Process Image)
        self.inputs: Dict[str, Any] = {}      # Данные от IO Map (Modbus TCP)
        self.outputs: Dict[str, Any] = {}     # Команды в IO Map (Modbus TCP)
        self.memory: Dict[str, Any] = {}      # Внутренние переменные и уставки (M-область)
        self.hmi_commands: Dict[str, Any] = {}  # Входящие команды от Web-HMI

        self.diag = PLCDiagnostics()
        self._running: bool = False
        self._ws_clients: Set[Any] = set()

    @abstractmethod
    async def read_io(self):
        """Чтение входов из IO Map (Modbus TCP)"""
        pass

    @abstractmethod
    async def write_io(self):
        """Запись выходов в IO Map (Modbus TCP)"""
        pass

    @abstractmethod
    def logic(self):
        """Основная циклическая программа (выполняется синхронно и быстро)"""
        pass

    def on_hmi_command(self, cmd: str, value: Any):
        """Обработка команд от FastAPI / Web-HMI"""
        self.hmi_commands[cmd] = value

    def get_snapshot(self) -> dict:
        """Снимок состояния ПЛК для отправки в Web-HMI ('режим очков')"""
        return {
            "timestamp": time.time(),
            "diag": {
                "scan_count": self.diag.scan_count,
                "actual_scan_ms": self.diag.actual_scan_ms,
                "max_scan_ms": self.diag.max_scan_ms,
                "overruns": self.diag.overruns,
            },
            "inputs": self.inputs,
            "outputs": self.outputs,
            "memory": self.memory,
        }

    # --- Внутренний WebSocket-сервер для связи с FastAPI (Web-HMI) ---
    async def _ws_handler(self, websocket):
        self._ws_clients.add(websocket)
        try:
            async for message in websocket:
                data = json.loads(message)
                # Ожидаем формат: {"cmd": "START", "value": true} или {"set_mem": {"target_depth": 15}}
                if "cmd" in data:
                    self.on_hmi_command(data["cmd"], data.get("value", True))
                if "set_mem" in data and isinstance(data["set_mem"], dict):
                    self.memory.update(data["set_mem"])
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._ws_clients.discard(websocket)

    async def _telemetry_broadcaster(self):
        """Рассылка состояния в Web-HMI каждые 100 мс (10 Гц)"""
        while self._running:
            if self._ws_clients:
                payload = json.dumps(self.get_snapshot(), default=str)
                await asyncio.gather(
                    *(client.send(payload) for client in self._ws_clients),
                    return_exceptions=True
                )
            await asyncio.sleep(0.1)

    # --- Главный цикл сканирования ПЛК ---
    async def _scan_loop(self):
        while self._running:
            cycle_start = time.monotonic()

            # 1. Чтение входов (Input Image)
            await self.read_io()

            # 2. Выполнение пользовательской логики
            self.logic()

            # 3. Запись выходов (Output Image)
            await self.write_io()

            # 4. Сброс импульсных команд HMI (если нужно) и расчет диагностики
            self.hmi_commands.clear()
            elapsed = time.monotonic() - cycle_start
            elapsed_ms = round(elapsed * 1000, 2)

            self.diag.scan_count += 1
            self.diag.actual_scan_ms = elapsed_ms
            if elapsed_ms > self.diag.max_scan_ms:
                self.diag.max_scan_ms = elapsed_ms

            sleep_time = self.scan_time_sec - elapsed
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                self.diag.overruns += 1
                await asyncio.sleep(0.001)  # Отдаем квант времени event loop

    async def start(self):
        """Запуск ядра ПЛК и сервера связи с HMI"""
        self._running = True
        ws_server = await websockets.serve(self._ws_handler, "127.0.0.1", self.hmi_ws_port)
        print(f"[PLC Core] Запущен. Цикл: {int(self.scan_time_sec*1000)} мс | HMI WS: ws://127.0.0.1:{self.hmi_ws_port}")
        try:
            await asyncio.gather(
                self._scan_loop(),
                self._telemetry_broadcaster(),
            )
        finally:
            ws_server.close()
            await ws_server.wait_closed()
