from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from .db_handler import Base


class ExtractMogi(Base):
    __tablename__ = "extract_mogi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nome_empresa = Column(String, nullable=False)
    telefone = Column(String, nullable=True)
    celular_whatsapp = Column(String, nullable=True)
    facebook_link = Column(String, nullable=True)
    email = Column(String, nullable=True)
    site = Column(String, nullable=True)
    data_extracao = Column(DateTime, server_default=func.now())
