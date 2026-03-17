"""
Database Initialization and Models for Card Tracker
"""

import sqlite3
from pathlib import Path
from config import CARDS_DB
from modules.logger import logger

def init_database():
    """
    Initialize the SQLite database with all required tables
    """
    try:
        conn = sqlite3.connect(CARDS_DB)
        cursor = conn.cursor()
        
        # Cards table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cards (
                uuid TEXT PRIMARY KEY,
                player_name TEXT NOT NULL,
                display_position TEXT,
                display_secondary_positions TEXT,
                team TEXT,
                team_short_name TEXT,
                ovr INTEGER,
                rarity TEXT,
                series TEXT,
                series_year INTEGER,
                card_image_url TEXT,
                card_type TEXT,
                
                -- Player Bio
                jersey_number TEXT,
                age INTEGER,
                bat_hand TEXT,
                throw_hand TEXT,
                height TEXT,
                weight TEXT,
                born TEXT,
                is_hitter BOOLEAN,
                
                -- Hitting Attributes
                contact_right INTEGER,
                contact_left INTEGER,
                power_right INTEGER,
                power_left INTEGER,
                plate_vision INTEGER,
                plate_discipline INTEGER,
                batting_clutch INTEGER,
                bunting_ability INTEGER,
                drag_bunting_ability INTEGER,
                
                -- Fielding Attributes
                fielding_ability INTEGER,
                arm_strength INTEGER,
                arm_accuracy INTEGER,
                reaction_time INTEGER,
                blocking INTEGER,
                fielding_durability INTEGER,
                
                -- Running Attributes
                speed INTEGER,
                baserunning_ability INTEGER,
                baserunning_aggression INTEGER,
                steal INTEGER,
                
                -- Pitching Attributes
                stamina INTEGER,
                pitching_clutch INTEGER,
                hits_per_9 INTEGER,
                k_per_9 INTEGER,
                bb_per_9 INTEGER,
                hr_per_9 INTEGER,
                pitch_velocity INTEGER,
                pitch_control INTEGER,
                pitch_movement INTEGER,
                hitting_durability INTEGER,
                
                -- Pitch Repertoire and Quirks (JSON)
                pitches TEXT,
                quirks TEXT,
                
                -- Card Metadata
                locations TEXT,
                event_eligible BOOLEAN,
                supercharged BOOLEAN,
                inside_edge_stars INTEGER,
                
                -- User-Entered Fields
                purchased_price INTEGER,
                current_sell_price INTEGER DEFAULT 0,
                current_buy_price INTEGER DEFAULT 0,
                sold_price INTEGER,
                quantity INTEGER DEFAULT 1,
                on_team BOOLEAN DEFAULT 0,
                grind_card BOOLEAN DEFAULT 0,
                pxp INTEGER DEFAULT 0,
                comments TEXT,
                inside_edge TEXT,
                card_status TEXT DEFAULT 'Active',
                
                -- Calculated Fields (stored for performance)
                profit_generated REAL DEFAULT 0,
                potential_profit REAL DEFAULT 0,
                total_investment INTEGER DEFAULT 0,
                
                -- Timestamps
                date_acquired TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_refreshed TIMESTAMP,
                
                -- Index fields
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Price history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_uuid TEXT NOT NULL,
                best_sell_price INTEGER,
                best_buy_price INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (card_uuid) REFERENCES cards(uuid) ON DELETE CASCADE
            )
        ''')
        
        # Attribute changes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS attribute_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_uuid TEXT NOT NULL,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (card_uuid) REFERENCES cards(uuid) ON DELETE CASCADE
            )
        ''')
        
        # Sell history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sell_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_uuid TEXT NOT NULL,
                quantity_sold INTEGER NOT NULL,
                sold_price_per_card INTEGER NOT NULL,
                profit REAL NOT NULL,
                sold_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (card_uuid) REFERENCES cards(uuid) ON DELETE CASCADE
            )
        ''')
        
        # Dynamic selections table (for user-added dropdown options)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dynamic_selections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                field_name TEXT NOT NULL,
                value TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(field_name, value)
            )
        ''')
        
        # Card catalog cache table (for local search across all cards)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS card_catalog (
                uuid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                rarity TEXT,
                team TEXT,
                ovr INTEGER,
                series TEXT,
                display_position TEXT,
                display_secondary_positions TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_sellable BOOLEAN DEFAULT 0
            )
        ''')
        
        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cards_rarity ON cards(rarity)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cards_position ON cards(display_position)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cards_team ON cards(team)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cards_series ON cards(series)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cards_status ON cards(card_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_uuid ON price_history(card_uuid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_attribute_changes_uuid ON attribute_changes(card_uuid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sell_history_uuid ON sell_history(card_uuid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_catalog_name ON card_catalog(name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_catalog_rarity ON card_catalog(rarity)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_catalog_position ON card_catalog(display_position)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_catalog_team ON card_catalog(team)')
        
        # Migration: Add pxp column if it doesn't exist
        try:
            cursor.execute("SELECT pxp FROM cards LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding pxp column to cards table...")
            cursor.execute("ALTER TABLE cards ADD COLUMN pxp INTEGER DEFAULT 0")
            conn.commit()
            logger.info("pxp column added successfully")

        # Migration: Add display_secondary_positions to card_catalog if missing
        try:
            cursor.execute("SELECT display_secondary_positions FROM card_catalog LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding display_secondary_positions column to card_catalog table...")
            cursor.execute("ALTER TABLE card_catalog ADD COLUMN display_secondary_positions TEXT")
            conn.commit()
            logger.info("display_secondary_positions column added successfully")
        
        conn.commit()
        conn.close()
        
        logger.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

def get_db_connection():
    """
    Get a database connection with row factory
    """
    conn = sqlite3.connect(CARDS_DB)
    conn.row_factory = sqlite3.Row
    return conn

# Initialize database on module import (safe to always run — all statements use IF NOT EXISTS)
init_database()
