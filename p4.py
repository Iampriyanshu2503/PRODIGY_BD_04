from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import redis
import re
import uuid
import os
import json

app = Flask(__name__)

# Database configuration with connection pooling
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///users.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "pool_pre_ping": True,
    "pool_recycle": 300
}

db = SQLAlchemy(app)

# Redis configuration
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
CACHE_TIMEOUT = 300  # Cache expiration time in seconds

# User model
class User(db.Model):
    id = db.Column(db.String(36), primary_key=True, index=True)
    name = db.Column(db.String(80), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    age = db.Column(db.Integer, nullable=False)

def is_valid_email(email):
    return re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$', email) is not None

@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json()
    if not data or not all(k in data for k in ('name', 'email', 'age')):
        return jsonify({'error': 'Missing fields'}), 400
    if not is_valid_email(data['email']):
        return jsonify({'error': 'Invalid email'}), 400
    if not isinstance(data['age'], int) or data['age'] <= 0:
        return jsonify({'error': 'Age must be a positive integer'}), 400

    user = User(id=str(uuid.uuid4()), name=data['name'], email=data['email'], age=data['age'])
    try:
        db.session.add(user)
        db.session.commit()
        redis_client.setex(f'user:{user.id}', CACHE_TIMEOUT, json.dumps({'id': user.id, 'name': user.name, 'email': user.email, 'age': user.age}))
        return jsonify({'id': user.id, 'name': user.name, 'email': user.email, 'age': user.age}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({'error': 'Email already exists'}), 400
    except SQLAlchemyError:
        db.session.rollback()
        return jsonify({'error': 'Database error'}), 500

@app.route('/users/<user_id>', methods=['GET'])
def get_user(user_id):
    cached_user = redis_client.get(f'user:{user_id}')
    if cached_user:
        return jsonify(json.loads(cached_user)), 200

    user = User.query.with_entities(User.id, User.name, User.email, User.age).filter_by(id=user_id).first()
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    user_data = {'id': user.id, 'name': user.name, 'email': user.email, 'age': user.age}
    redis_client.setex(f'user:{user.id}', CACHE_TIMEOUT, json.dumps(user_data))
    return jsonify(user_data), 200

@app.route('/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    if 'name' in data:
        user.name = data['name']
    if 'email' in data:
        if not is_valid_email(data['email']):
            return jsonify({'error': 'Invalid email'}), 400
        user.email = data['email']
    if 'age' in data:
        if not isinstance(data['age'], int) or data['age'] <= 0:
            return jsonify({'error': 'Age must be a positive integer'}), 400
        user.age = data['age']

    try:
        db.session.commit()
        redis_client.delete(f'user:{user.id}')  # Invalidate cache
        return jsonify({'id': user.id, 'name': user.name, 'email': user.email, 'age': user.age}), 200
    except IntegrityError:
        db.session.rollback()
        return jsonify({'error': 'Email already exists'}), 400
    except SQLAlchemyError:
        db.session.rollback()
        return jsonify({'error': 'Database error'}), 500

@app.route('/users/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    db.session.delete(user)
    try:
        db.session.commit()
        redis_client.delete(f'user:{user.id}')  # Remove from cache
        return jsonify({'message': 'User deleted successfully'}), 200
    except SQLAlchemyError:
        db.session.rollback()
        return jsonify({'error': 'Database error'}), 500

@app.route('/users', methods=['GET'])
def get_all_users():
    users = User.query.with_entities(User.id, User.name, User.email, User.age).all()
    return jsonify([{'id': u.id, 'name': u.name, 'email': u.email, 'age': u.age} for u in users]), 200

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true')
