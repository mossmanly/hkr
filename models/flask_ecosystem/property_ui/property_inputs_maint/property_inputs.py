"""
Property Editor Flask Application
Phase II of HKH Property Management Ecosystem

Uses shared components for database, config, and utilities.
Implements two-step navigation: Portfolio -> Property selection.
Accordion-based form with Header always visible.
"""

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime

# Import shared components (adjust path as needed for your DBT structure)
# from shared_components import get_db_connection, get_config
# from shared_components.utils import trigger_dbt_refresh, trigger_metabase_refresh

# Temporary database config for development
# Replace with shared_components.database.get_db_connection() once integrated
DATABASE_CONFIG = {
    'host': 'dpg-d0glfhjuibrs73fnvht0-a.oregon-postgres.render.com',
    'database': 'hkh_decision_support_db',
    'user': 'moss',
    'port': 5432,
    'sslmode': 'require'
}

def get_db_connection():
    """Temporary database connection - replace with shared component"""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

# Flask app configuration
app = Flask(__name__)
app.config['SECRET_KEY'] = 'temp-secret-key-replace-with-shared-config'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# Set current company ID (hardcoded for Phase II)
CURRENT_COMPANY_ID = 1

@app.route('/')
def index():
    """Main landing page - portfolio selection"""
    try:
        conn = get_db_connection()
        if not conn:
            flash('Database connection failed', 'error')
            return render_template('error.html')
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get portfolios for current company
        cursor.execute("""
            SELECT DISTINCT portfolio_id 
            FROM hkh_dev.stg_property_inputs 
            WHERE company_id = %s AND portfolio_id IS NOT NULL
            ORDER BY portfolio_id
        """, (CURRENT_COMPANY_ID,))
        
        portfolios = cursor.fetchall()
        conn.close()
        
        return render_template('selection/portfolio_select.html', portfolios=portfolios)
        
    except Exception as e:
        logging.error(f"Error loading portfolios: {e}")
        flash('Error loading portfolios', 'error')
        return render_template('error.html')

@app.route('/portfolio/<portfolio_id>')
def property_select(portfolio_id):
    """Property selection within a portfolio"""
    try:
        conn = get_db_connection()
        if not conn:
            flash('Database connection failed', 'error')
            return redirect(url_for('index'))
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get properties in selected portfolio
        cursor.execute("""
            SELECT property_id, property_name, property_address, city, unit_count, building_class
            FROM hkh_dev.stg_property_inputs 
            WHERE company_id = %s AND portfolio_id = %s AND property_id IS NOT NULL
            ORDER BY property_name
        """, (CURRENT_COMPANY_ID, portfolio_id))
        
        properties = cursor.fetchall()
        conn.close()
        
        if not properties:
            flash(f'No properties found in portfolio {portfolio_id}', 'warning')
            return redirect(url_for('index'))
        
        return render_template('selection/property_select.html', 
                             portfolio_id=portfolio_id, 
                             properties=properties)
        
    except Exception as e:
        logging.error(f"Error loading properties: {e}")
        flash('Error loading properties', 'error')
        return redirect(url_for('index'))

@app.route('/edit/<portfolio_id>/<property_id>')
def property_edit(portfolio_id, property_id):
    """Property edit form with accordion sections"""
    try:
        conn = get_db_connection()
        if not conn:
            flash('Database connection failed', 'error')
            return redirect(url_for('index'))
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get full property data
        cursor.execute("""
            SELECT * FROM hkh_dev.stg_property_inputs 
            WHERE company_id = %s AND portfolio_id = %s AND property_id = %s
        """, (CURRENT_COMPANY_ID, portfolio_id, property_id))
        
        property_data = cursor.fetchone()
        conn.close()
        
        if not property_data:
            flash(f'Property {property_id} not found', 'error')
            return redirect(url_for('property_select', portfolio_id=portfolio_id))
        
        return render_template('editing/property_edit.html', 
                             portfolio_id=portfolio_id,
                             property=property_data)
        
    except Exception as e:
        logging.error(f"Error loading property: {e}")
        flash('Error loading property data', 'error')
        return redirect(url_for('property_select', portfolio_id=portfolio_id))

@app.route('/update/<portfolio_id>/<property_id>', methods=['POST'])
def property_update(portfolio_id, property_id):
    """Update property data"""
    try:
        conn = get_db_connection()
        if not conn:
            flash('Database connection failed', 'error')
            return redirect(url_for('property_edit', portfolio_id=portfolio_id, property_id=property_id))
        
        cursor = conn.cursor()
        
        # Build update query dynamically based on form data
        # This will be populated based on the form fields we create
        update_fields = []
        update_values = []
        
        # Header fields
        for field in ['property_name', 'property_address', 'city', 'zip', 'building_class']:
            if field in request.form and request.form[field].strip():
                update_fields.append(f"{field} = %s")
                update_values.append(request.form[field].strip())
        
        # Property Traits
        for field in ['unit_count', 'capex_per_unit']:
            if field in request.form and request.form[field].strip():
                update_fields.append(f"{field} = %s")
                try:
                    value = float(request.form[field]) if '.' in request.form[field] else int(request.form[field])
                    update_values.append(value)
                except ValueError:
                    flash(f'Invalid value for {field}', 'error')
                    return redirect(url_for('property_edit', portfolio_id=portfolio_id, property_id=property_id))
        
        # Add more field groups as we build them...
        
        if update_fields:
            update_values.extend([CURRENT_COMPANY_ID, portfolio_id, property_id])
            
            query = f"""
                UPDATE hkh_dev.stg_property_inputs 
                SET {', '.join(update_fields)}, staging_loaded_at = NOW()
                WHERE company_id = %s AND portfolio_id = %s AND property_id = %s
            """
            
            cursor.execute(query, update_values)
            conn.commit()
            
            flash('Property updated successfully!', 'success')
            
            # TODO: Trigger DBT refresh using shared components
            # trigger_dbt_refresh()
            # trigger_metabase_refresh()
        
        conn.close()
        return redirect(url_for('property_edit', portfolio_id=portfolio_id, property_id=property_id))
        
    except Exception as e:
        logging.error(f"Error updating property: {e}")
        flash('Error updating property', 'error')
        return redirect(url_for('property_edit', portfolio_id=portfolio_id, property_id=property_id))

@app.errorhandler(404)
def not_found(error):
    return render_template('shared/error.html', error="Page not found"), 404

@app.errorhandler(500)
def server_error(error):
    return render_template('shared/error.html', error="Server error"), 500

if __name__ == '__main__':
    # Run on port 5002 (existing app on 5001)
    app.run(debug=True, host='0.0.0.0', port=5002)