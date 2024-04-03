from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, select
from sqlalchemy.orm import DeclarativeBase, relationship, Session

import pandas as pd


class Base(DeclarativeBase):
    pass


class Cliente(Base):
    __tablename__ = 'cliente'

    id = Column(Integer, primary_key=True)
    nome = Column(String, )
    cpf = Column(String(9), )
    cep = Column(String(9), )

    contas = relationship(
        'Conta',  # Target Model Class
        back_populates='cliente'  # Target Relationship
    )

    def __repr__(self):
        return f"Cliente(id={self.id}, name={self.nome}, cpf={self.cpf}, cep={self.cep})"


class Conta(Base):
    __tablename__ = 'conta'

    id = Column(Integer, primary_key=True)
    tipo = Column(String, )
    agencia = Column(String, )
    num = Column(String, )
    saldo = Column(Float, )
    id_cliente = Column(Integer, ForeignKey('cliente.id'), nullable=False)

    cliente = relationship(
        'Cliente',
        back_populates='contas'
    )

    def __repr__(self):
        return ("Conta(id={}, tipo={}, agencia={}, num={}, saldo={})"
                .format(self.id, self.tipo, self.agencia, self.num, self.saldo))


def add_data(eng):
    df_clientes = pd.read_csv('./temp/clientes.csv')
    df_contas = pd.read_csv('./temp/contas.csv')

    df_clientes.to_sql(
        Cliente.__tablename__,
        con=eng,
        if_exists='replace',
        index=False
    )

    df_contas.to_sql(
        Conta.__tablename__,
        con=eng,
        if_exists='replace',
        index=False
    )

    with Session(eng) as session:
        session.commit()


def print_stmt_with_execute(stmt, eng):
    """
        Oferece mais flexibilidade, imprimindo o objeto ResultProxy e permitindo a iteração por registros individuais.
        Executa a instrução SQL e retorna um objeto de resultado do SQLAlchemy.
        Este objeto contém todos os dados recuperados da consulta,
        incluindo os valores e os metadados (nomes de colunas)
            :param stmt: intrução sql
            :param eng: engine com a conexão ao BD
    """
    print('>>>')
    with Session(eng) as session:
        results = session.execute(stmt)
        for record in results:
            print(record)
    print('-'*50)


if __name__ == '__main__':
    engine = create_engine("sqlite+pysqlite:///sqlite.db", echo=True)

    Base.metadata.create_all(engine)

    add_data(engine)

    print('\nRecuperando clientes')
    stmt_clientes = select(Cliente)
    print_stmt_with_execute(stmt_clientes, engine)

    print('\nRecuperando contas')
    stmt_contas = select(Conta)
    print_stmt_with_execute(stmt_contas, engine)
