import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    # 📁 สร้างโฟลเดอร์ logs ถ้ายังไม่มี
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    # 🗂️ ตั้งชื่อไฟล์ตามวันที่
    log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    # 🧱 สร้าง logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # หรือ DEBUG สำหรับรายละเอียดมากขึ้น
    logger.propagate = False

    # ป้องกัน logger ซ้ำซ้อน (ถ้าเคยสร้างไปแล้ว)
    if not logger.handlers:
        # 📄 Handler เขียนลงไฟล์แบบหมุนรายวัน (เก็บไว้ 7 วัน)
        file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7, encoding="utf-8")
        file_handler.suffix = "%Y-%m-%d"

        # 🖥️ Handler แสดงบน console
        console_handler = logging.StreamHandler()

        # 📋 ตั้ง format
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
