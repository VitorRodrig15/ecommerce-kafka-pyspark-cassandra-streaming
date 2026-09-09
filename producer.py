import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

# Inicializa o gerador de dados fictícios em PT-BR
fake = Faker('pt_BR')

# Lista de produtos com preços pré-definidos (Simulação Mercado Livre / Amazon)
PRODUTOS_CATALOGO = [
    {"nome": "Smartphone Samsung Galaxy S23", "preco": 3499.00},
    {"nome": "Notebook Dell Inspiron 15", "preco": 4200.00},
    {"nome": "Fone de Ouvido Bluetooth JBL", "preco": 299.90},
    {"nome": "Smart TV 55 4K LG", "preco": 2899.00},
    {"nome": "Teclado Mecânico Gamer Redragon", "preco": 250.00},
    {"nome": "Mouse Sem Fio Logitech", "preco": 120.00},
    {"nome": "Monitor Gamer AOC 24 144Hz", "preco": 999.00},
    {"nome": "Cadeira Gamer Ergoclass", "preco": 850.00}
]

def criar_produtor():
    """Conecta ao broker do Kafka"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def gerar_venda():
    """Gera uma estrutura de dados de venda contendo todos os requisitos da Parte 02"""
    
    # Seleciona de 1 a 3 produtos aleatórios para a mesma compra
    produtos_selecionados = random.sample(PRODUTOS_CATALOGO, k=random.randint(1, 3))
    
    itens_compra = []
    valor_total_venda = 0.0

    for item in produtos_selecionados:
        qtd = random.randint(1, 4)
        subtotal = round(item["preco"] * qtd, 2)
        valor_total_venda += subtotal
        
        itens_compra.append({
            "nome_produto": item["nome"],
            "quantidade": qtd,
            "preco_unitario": item["preco"],
            "subtotal": subtotal
        })

    # Data e hora geradas no formato DD/MM/YYYY HH:MM:SS
    data_hora_formatada = fake.date_time_this_year().strftime("%d/%m/%Y %H:%M:%S")

    # Estrutura completa da mensagem de venda enviada ao Kafka
    venda = {
        "id_ordem": fake.uuid4(),
        "documento_cliente": fake.cpf(),
        "produtos_comprados": itens_compra,
        "quantidade_total_itens": sum(i["quantidade"] for i in itens_compra),
        "valor_total_venda": round(valor_total_venda, 2),
        "data_hora_venda": data_hora_formatada
    }
    
    return venda

if __name__ == "__main__":
    producer = criar_produtor()
    print("🚀 Producer de Vendas E-commerce iniciado... Enviando mensagens para o Kafka!")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            venda = gerar_venda()
            
            # Envia a mensagem em formato JSON para o tópico 'e-commerce-vendas'
            producer.send('e-commerce-vendas', value=venda)
            
            # Exibe no terminal todos os dados detalhados para verificação
            nomes_produtos = ", ".join([p["nome_produto"] for p in venda["produtos_comprados"]])
            
            print(f"📦 [VENDA ENVIADA]")
            print(f"   ├─ ID Ordem: {venda['id_ordem']}")
            print(f"   ├─ Data/Hora: {venda['data_hora_venda']}")
            print(f"   ├─ Cliente CPF: {venda['documento_cliente']}")
            print(f"   ├─ Produtos: {nomes_produtos}")
            print(f"   └─ Valor Total: R$ {venda['valor_total_venda']:.2f}\n")
            
            # Intervalo aleatório entre as vendas simuladas
            time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("\n🛑 Producer interrompido pelo usuário.")
    finally:
        producer.flush()
        producer.close()