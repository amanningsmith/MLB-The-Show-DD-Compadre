"""
Automatic Backup System
"""

import shutil
from datetime import datetime
from pathlib import Path
from config import BACKUP_DIR, MAX_BACKUPS, MISSIONS_DB, CARDS_DB
from modules.logger import logger

def create_backup(file_path, operation_name='manual'):
    """
    Create a timestamped backup of a file
    
    Args:
        file_path: Path to the file to backup
        operation_name: Name of the operation triggering the backup
        
    Returns:
        Path to the backup file or None if failed
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.warning(f"Cannot backup {file_path.name} - file does not exist")
            return None
        
        # Create timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create backup filename
        backup_name = f"{file_path.stem}_backup_{timestamp}{file_path.suffix}"
        backup_path = BACKUP_DIR / backup_name
        
        # Copy file
        shutil.copy2(file_path, backup_path)
        
        logger.info(f"Backup created: {backup_name} (operation: {operation_name})")
        
        # Clean old backups
        cleanup_old_backups(file_path.stem)
        
        return backup_path
        
    except Exception as e:
        logger.error(f"Backup failed for {file_path}: {e}")
        return None

def cleanup_old_backups(file_stem):
    """
    Remove old backups, keeping only MAX_BACKUPS most recent
    
    Args:
        file_stem: The stem of the original file (e.g., 'missions', 'cards')
    """
    try:
        # Find all backups for this file
        backups = sorted(
            BACKUP_DIR.glob(f"{file_stem}_backup_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        # Remove old backups
        for old_backup in backups[MAX_BACKUPS:]:
            old_backup.unlink()
            logger.info(f"Removed old backup: {old_backup.name}")
            
    except Exception as e:
        logger.error(f"Backup cleanup failed: {e}")

def backup_missions(operation_name='manual'):
    """Backup missions database"""
    return create_backup(MISSIONS_DB, operation_name)

def backup_cards(operation_name='manual'):
    """Backup cards database"""
    return create_backup(CARDS_DB, operation_name)

def backup_all(operation_name='manual'):
    """Backup all data files"""
    results = {
        'missions': backup_missions(operation_name),
        'cards': backup_cards(operation_name)
    }
    return results
