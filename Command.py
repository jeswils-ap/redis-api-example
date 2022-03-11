from pydantic import BaseModel


class Command(BaseModel):
	command_value: str

	class Config:
		orm_mode = True