from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime, date
import json
from decimal import Decimal
from datetime import datetime, date, timedelta
from flask import send_file

app = Flask(__name__)
CORS(app)

# Configuração da base de dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'minigolfe_portugal'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'Paulinha02'),
    'charset': 'utf8mb4',
    'use_unicode': True
}

def get_db_connection():
    """Criar conexão com a base de dados"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Erro ao conectar com MySQL: {e}")
        return None

def serialize_data(obj):
    """Converter tipos especiais para JSON"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    elif isinstance(obj, timedelta):
        # Converter timedelta para string no formato HH:MM:SS
        total_seconds = int(obj.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return obj

def dict_factory(cursor, row):
    """Converter resultado do cursor em dicionário"""
    return {col[0]: serialize_data(val) for col, val in zip(cursor.description, row)}

@app.route('/')
def serve_html():
    return send_file('exp.html')

# ==================== CIDADES ====================

@app.route('/api/cidades', methods=['GET'])
def get_cidades():
    """Listar todas as cidades"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM cidades ORDER BY nome")
        cidades = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(cidades)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/cidades', methods=['POST'])
def create_cidade():
    """Criar nova cidade"""
    data = request.get_json()
    if not data or 'nome' not in data or 'distrito' not in data:
        return jsonify({'error': 'Nome e distrito são obrigatórios'}), 400
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = "INSERT INTO cidades (nome, distrito, codigo_postal) VALUES (%s, %s, %s)"
        cursor.execute(query, (data['nome'], data['distrito'], data.get('codigo_postal')))
        connection.commit()
        return jsonify({'id': cursor.lastrowid, 'message': 'Cidade criada com sucesso'}), 201
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== CAMPOS ====================

@app.route('/api/campos', methods=['GET'])
def get_campos():
    """Listar todos os campos"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        SELECT c.*, ci.nome as cidade_nome, ci.distrito,
               COUNT(p.id) as total_pistas
        FROM campos c
        LEFT JOIN cidades ci ON c.cidade_id = ci.id
        LEFT JOIN pistas p ON c.id = p.campo_id AND p.ativa = TRUE
        WHERE c.ativo = TRUE
        GROUP BY c.id
        ORDER BY ci.nome, c.nome
        """
        cursor.execute(query)
        campos = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(campos)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/campos/<int:campo_id>', methods=['GET'])
def get_campo(campo_id):
    """Obter detalhes de um campo específico"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        SELECT c.*, ci.nome as cidade_nome, ci.distrito
        FROM campos c
        LEFT JOIN cidades ci ON c.cidade_id = ci.id
        WHERE c.id = %s AND c.ativo = TRUE
        """
        cursor.execute(query, (campo_id,))
        campo = cursor.fetchone()
        
        if not campo:
            return jsonify({'error': 'Campo não encontrado'}), 404
            
        campo_dict = dict_factory(cursor, campo)
        
        # Buscar pistas do campo
        cursor.execute("""
        SELECT * FROM pistas 
        WHERE campo_id = %s AND ativa = TRUE 
        ORDER BY numero_pista
        """, (campo_id,))
        pistas = [dict_factory(cursor, row) for row in cursor.fetchall()]
        campo_dict['pistas'] = pistas
        
        return jsonify(campo_dict)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/campos', methods=['POST'])
def create_campo():
    """Criar novo campo"""
    data = request.get_json()
    required_fields = ['nome', 'cidade_id', 'tipo']
    
    if not data or not all(field in data for field in required_fields):
        return jsonify({'error': 'Nome, cidade_id e tipo são obrigatórios'}), 400
    
    if data['tipo'] not in ['petergolfe', 'feltgolfe', 'minigolfe']:
        return jsonify({'error': 'Tipo deve ser: petergolfe, feltgolfe ou minigolfe'}), 400
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO campos (nome, cidade_id, tipo, endereco, telefone, website, email,
                           latitude, longitude, preco_adulto, preco_crianca, 
                           horario_abertura, horario_fecho)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            data['nome'], data['cidade_id'], data['tipo'],
            data.get('endereco'), data.get('telefone'), data.get('website'),
            data.get('email'), data.get('latitude'), data.get('longitude'),
            data.get('preco_adulto'), data.get('preco_crianca'),
            data.get('horario_abertura'), data.get('horario_fecho')
        )
        cursor.execute(query, values)
        connection.commit()
        return jsonify({'id': cursor.lastrowid, 'message': 'Campo criado com sucesso'}), 201
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== PISTAS ====================

@app.route('/api/campos/<int:campo_id>/pistas', methods=['GET'])
def get_pistas_campo(campo_id):
    """Listar pistas de um campo"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        SELECT p.*, c.nome as campo_nome
        FROM pistas p
        JOIN campos c ON p.campo_id = c.id
        WHERE p.campo_id = %s AND p.ativa = TRUE
        ORDER BY p.numero_pista
        """
        cursor.execute(query, (campo_id,))
        pistas = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(pistas)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/pistas', methods=['POST'])
def create_pista():
    """Criar nova pista"""
    data = request.get_json()
    required_fields = ['campo_id', 'numero_pista']
    
    if not data or not all(field in data for field in required_fields):
        return jsonify({'error': 'campo_id e numero_pista são obrigatórios'}), 400
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO pistas (campo_id, numero_pista, nome, dificuldade, par, descricao)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            data['campo_id'], data['numero_pista'], data.get('nome'),
            data.get('dificuldade', 'medio'), data.get('par', 3), data.get('descricao')
        )
        cursor.execute(query, values)
        connection.commit()
        return jsonify({'id': cursor.lastrowid, 'message': 'Pista criada com sucesso'}), 201
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== JOGADORES ====================

@app.route('/api/jogadores', methods=['GET'])
def get_jogadores():
    """Listar todos os jogadores"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        SELECT j.*, c.nome as cidade_nome
        FROM jogadores j
        LEFT JOIN cidades c ON j.cidade_id = c.id
        ORDER BY j.nome
        """
        cursor.execute(query)
        jogadores = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(jogadores)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/jogadores', methods=['POST'])
def create_jogador():
    """Criar novo jogador"""
    data = request.get_json()
    if not data or 'nome' not in data:
        return jsonify({'error': 'Nome é obrigatório'}), 400
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO jogadores (nome, email, telefone, data_nascimento, cidade_id, avatar_url)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            data['nome'], data.get('email'), data.get('telefone'),
            data.get('data_nascimento'), data.get('cidade_id'), data.get('avatar_url')
        )
        cursor.execute(query, values)
        connection.commit()
        return jsonify({'id': cursor.lastrowid, 'message': 'Jogador criado com sucesso'}), 201
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== JOGOS ====================

@app.route('/api/jogos', methods=['GET'])
def get_jogos():
    """Listar todos os jogos"""
    campo_id = request.args.get('campo_id')
    limit = request.args.get('limit', 50)
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        SELECT j.*, c.nome as campo_nome, ci.nome as cidade_nome
        FROM jogos j
        JOIN campos c ON j.campo_id = c.id
        JOIN cidades ci ON c.cidade_id = ci.id
        """
        params = []
        
        if campo_id:
            query += " WHERE j.campo_id = %s"
            params.append(campo_id)
        
        query += " ORDER BY j.data_jogo DESC LIMIT %s"
        params.append(int(limit))
        
        cursor.execute(query, params)
        jogos = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(jogos)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/jogos', methods=['POST'])
def create_jogo():
    """Criar novo jogo"""
    data = request.get_json()
    required_fields = ['campo_id', 'jogadores']
    
    if not data or not all(field in data for field in required_fields):
        return jsonify({'error': 'campo_id e jogadores são obrigatórios'}), 400
    
    if not isinstance(data['jogadores'], list) or len(data['jogadores']) == 0:
        return jsonify({'error': 'Deve haver pelo menos um jogador'}), 400
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        connection.start_transaction()
        
        # Criar o jogo
        query_jogo = """
        INSERT INTO jogos (campo_id, data_jogo, num_jogadores, observacoes)
        VALUES (%s, %s, %s, %s)
        """
        data_jogo = data.get('data_jogo', datetime.now().isoformat())
        values_jogo = (
            data['campo_id'], data_jogo, len(data['jogadores']), data.get('observacoes')
        )
        cursor.execute(query_jogo, values_jogo)
        jogo_id = cursor.lastrowid
        
        # Adicionar participantes
        query_participante = """
        INSERT INTO jogo_participantes (jogo_id, jogador_id, ordem_jogador)
        VALUES (%s, %s, %s)
        """
        for i, jogador_id in enumerate(data['jogadores'], 1):
            cursor.execute(query_participante, (jogo_id, jogador_id, i))
        
        connection.commit()
        return jsonify({'id': jogo_id, 'message': 'Jogo criado com sucesso'}), 201
    except Error as e:
        connection.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/jogos/<int:jogo_id>', methods=['GET'])
def get_jogo(jogo_id):
    """Obter detalhes de um jogo específico"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        
        # Buscar jogo
        query_jogo = """
        SELECT j.*, c.nome as campo_nome, ci.nome as cidade_nome
        FROM jogos j
        JOIN campos c ON j.campo_id = c.id
        JOIN cidades ci ON c.cidade_id = ci.id
        WHERE j.id = %s
        """
        cursor.execute(query_jogo, (jogo_id,))
        jogo = cursor.fetchone()
        
        if not jogo:
            return jsonify({'error': 'Jogo não encontrado'}), 404
        
        jogo_dict = dict_factory(cursor, jogo)
        
        # Buscar participantes
        query_participantes = """
        SELECT jp.*, jog.nome as jogador_nome
        FROM jogo_participantes jp
        JOIN jogadores jog ON jp.jogador_id = jog.id
        WHERE jp.jogo_id = %s
        ORDER BY jp.ordem_jogador
        """
        cursor.execute(query_participantes, (jogo_id,))
        participantes = [dict_factory(cursor, row) for row in cursor.fetchall()]
        jogo_dict['participantes'] = participantes
        
        # Buscar tacadas
        query_tacadas = """
        SELECT t.*, p.numero_pista, p.nome as pista_nome, jog.nome as jogador_nome
        FROM tacadas t
        JOIN pistas p ON t.pista_id = p.id
        JOIN jogadores jog ON t.jogador_id = jog.id
        WHERE t.jogo_id = %s
        ORDER BY p.numero_pista, t.jogador_id
        """
        cursor.execute(query_tacadas, (jogo_id,))
        tacadas = [dict_factory(cursor, row) for row in cursor.fetchall()]
        jogo_dict['tacadas'] = tacadas
        
        return jsonify(jogo_dict)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== TACADAS ====================

@app.route('/api/tacadas', methods=['POST'])
def registrar_tacada():
    """Registrar tacada de um jogador numa pista"""
    data = request.get_json()
    required_fields = ['jogo_id', 'jogador_id', 'pista_id', 'numero_tacadas']
    
    if not data or not all(field in data for field in required_fields):
        return jsonify({'error': 'jogo_id, jogador_id, pista_id e numero_tacadas são obrigatórios'}), 400
    
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO tacadas (jogo_id, jogador_id, pista_id, numero_tacadas, tempo_pista, observacoes)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        numero_tacadas = VALUES(numero_tacadas),
        tempo_pista = VALUES(tempo_pista),
        observacoes = VALUES(observacoes)
        """
        values = (
            data['jogo_id'], data['jogador_id'], data['pista_id'], data['numero_tacadas'],
            data.get('tempo_pista'), data.get('observacoes')
        )
        cursor.execute(query, values)
        connection.commit()
        
        # Recalcular estatísticas do jogo
        cursor.callproc('CalcularEstatisticasJogo', [data['jogo_id']])
        connection.commit()
        
        return jsonify({'message': 'Tacada registrada com sucesso'}), 201
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== ESTATÍSTICAS ====================

@app.route('/api/estatisticas/campos', methods=['GET'])
def get_estatisticas_campos():
    """Obter estatísticas dos campos"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM vw_estatisticas_campo")
        stats = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(stats)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/estatisticas/jogadores', methods=['GET'])
def get_ranking_jogadores():
    """Obter ranking dos jogadores"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM vw_ranking_jogadores LIMIT 20")
        ranking = [dict_factory(cursor, row) for row in cursor.fetchall()]
        return jsonify(ranking)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/estatisticas/jogador/<int:jogador_id>', methods=['GET'])
def get_estatisticas_jogador(jogador_id):
    """Obter estatísticas de um jogador específico"""
    connection = get_db_connection()
    if not connection:
        return jsonify({'error': 'Erro de conexão com a base de dados'}), 500
    
    try:
        cursor = connection.cursor()
        query = """
        SELECT 
            j.nome,
            COUNT(DISTINCT jp.jogo_id) as total_jogos,
            AVG(jp.total_tacadas) as media_tacadas,
            MIN(jp.total_tacadas) as melhor_score,
            MAX(jp.total_tacadas) as pior_score,
            COUNT(CASE WHEN jp.posicao_final = 1 THEN 1 END) as vitorias,
            MAX(jg.data_jogo) as ultimo_jogo
        FROM jogadores j
        LEFT JOIN jogo_participantes jp ON j.id = jp.jogador_id
        LEFT JOIN jogos jg ON jp.jogo_id = jg.id
        WHERE j.id = %s
        GROUP BY j.id, j.nome
        """
        cursor.execute(query, (jogador_id,))
        stats = cursor.fetchone()
        
        if not stats:
            return jsonify({'error': 'Jogador não encontrado'}), 404
        
        return jsonify(dict_factory(cursor, stats))
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# ==================== ENDPOINTS DE UTILIDADE ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Verificar se a API está funcionando"""
    connection = get_db_connection()
    if connection:
        connection.close()
        return jsonify({'status': 'OK', 'database': 'Connected'})
    else:
        return jsonify({'status': 'ERROR', 'database': 'Disconnected'}), 500

@app.route('/api/info', methods=['GET'])
def api_info():
    """Informações sobre a API"""
    return jsonify({
        'name': 'Minigolfe Portugal API',
        'version': '1.0.0',
        'description': 'API para gestão de campos de minigolfe em Portugal',
        'endpoints': {
            'cidades': '/api/cidades',
            'campos': '/api/campos',
            'pistas': '/api/pistas',
            'jogadores': '/api/jogadores',
            'jogos': '/api/jogos',
            'tacadas': '/api/tacadas',
            'estatisticas': '/api/estatisticas'
        }
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)