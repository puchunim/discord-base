import nextcord
from pydoc import locate
from inspect import getsource
from dataclasses import dataclass

async def desc_table(table_name, guild):
    table = nextcord.utils.get(guild.categories, name=table_name)
    table_info = {}
    
    if table:
        table_info["t"] = table
        table_info["name"] = table_name
        table_info["field_count"] = len(table.text_channels)
        table_info["fields"] = {c.name:locate(c.topic) for c in table.text_channels}
        table_info["id"] = nextcord.utils.get(table.text_channels, nsfw=True).name    
    
    return table_info

async def get_registers(table_name, id, guild):
    channel = nextcord.utils.get(
        nextcord.utils.get(
            guild.categories, 
            name=table_name
        ).text_channels,
        name=id
    )
    return [locate(channel.topic)(m.content) for m in await channel.history(limit=None).flatten()][::-1]

# TODO: class for tables? I NEED IT A LOOOT

class DiscordBase:
    def __init__(
        self,
        bot: nextcord.ext.commands.Bot,
        id: int
    ):
        self.bot = bot; self.id = id
        self.guild = bot.get_guild(id)
        if not self.guild:
            raise Exception("no guild found") # mudar
    
    async def cursor(
        self,
        action: str,
        **kwargs: dict
    ):
        acts: tuple = ("CREATE", "SELECT", "INSERT", "DELETE", "ALTER", "UPDATE")
        if not action.upper() in acts:
            raise Exception(f"no action type '{action.upper()}' found") # mudar
        
        table_name = kwargs.get("table")
        fields = kwargs.get("fields")
        where = kwargs.get("where")
        tb_info = await desc_table(table_name, self.guild)
        
        if table_name:
            if not tb_info:
                raise Exception(f"no table '{table_name}' found in this database")
        
        if fields:
            if not isinstance(fields, (dict, list, tuple)):
                raise Exception(f"the arg 'fields' only support iterables like <tup>, <dict> or <list>, not <{type(fields).__name__}>")
            
        if where:
            ops = (">=", "<=", "!=", ">", "<")
            clauses = []
            for k, v in where.items():
                field = k[::]
                op = "=="

                if "@" in field:
                    field, op = field.split("@")
                    if not op in ops:
                        raise Exception(f"operation '{op}' not acceptable")
                    
                if not field in list(tb_info["fields"].keys()):
                    raise Exception(f"field '{k}' not found in table '{table_name}'")
                
                clauses.append(f"x['{field}'] {op} {v!r}")
            clause = eval(f"lambda x: {' and '.join(clauses)}")
                
        match action.upper():
            case "CREATE":
                if table_name and not tb_info:
                    category = await self.guild.create_category(table_name)
                    for cn, ct in fields.items():
                        ch = await category.create_text_channel(
                            cn.strip("$"), 
                            topic=ct.__name__
                        )
                        if cn.startswith("$"):
                            await ch.edit(nsfw=True)

            case "SELECT": # campos, tabela, *where    
                not_found = [f for f in fields if not f in list(tb_info["fields"].keys())]
                
                if not_found:
                    raise Exception(f"fields not found in table: {', '.join(not_found)}")
                
                registers = []
                for f in fields:
                    
                    registers.append(await get_registers(
                        table_name,
                        f,
                        self.guild
                    ))
                
                return list(filter(clause, [dict(zip(fields, r)) for r in zip(*registers)]))
                
            case "INSERT": # tabela, campos            
                if isinstance(fields, (list, tuple)):
                    order = tuple(tb_info["fields"].keys())
                    types = tuple(tb_info["fields"].values())
                    values = fields
                    
                else:
                    order = tuple(fields.keys())
                    types = tuple([tb_info["fields"][p] for p in fields.keys()])
                    values = list(fields.values())
                
                for i, vt in enumerate(zip(values, types)):
                    if not isinstance(vt[0], vt[1]):
                        raise Exception(f"Field '{order[i]}' expected to be type <{vt[1].__name__}>, found <{type(vt[0]).__name__}>")
                    
                reg = await get_registers(table_name, tb_info["id"], self.guild)
                
                if values[order.index(tb_info["id"])] in reg:
                    raise Exception(f"Register with ID field '{values[order.index(tb_info['id'])]}' already in database")
                
                for i, cn in enumerate(order):
                    ch = nextcord.utils.get(tb_info["t"].text_channels, name=cn)
                    await ch.send(values[i])

            case "DELETE": # ??
                ...
            
            case "ALTER": # tabela, alteração estrutural
                ...
                
            case "UPDATE": # tabela, registro, alteração, *where?
                ...