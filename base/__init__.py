import nextcord

class DiscordBase:
    def __init__(
        self,
        bot: nextcord.ext.commands.Bot,
        id: int
    ):
        self.bot = bot; self.id = id
        self.guild = bot.get_guild(id)
        if not self.guild:
            raise ConnectionError("no guild found")
    
    async def cursor(
        self,
        action: str,
        **kwargs: dict
    ):
        acts: tuple = ("CREATE", "SELECT", "INSERT", "DELETE", "ALTER", "UPDATE")
        if not action.upper() in acts:
            raise TypeError(f"no action type '{action.upper()}' found")

        match action.upper():
            case "CREATE": # nome da tabela, campos
                table_name = kwargs.get("table")
                if table_name and not nextcord.utils.get(self.guild.categories, name=table_name):
                    category = await self.guild.create_category(table_name)
                    for cn, ct in kwargs.get("fields").items():
                        await category.create_text_channel(cn, topic=ct.__name__)

            case "SELECT": # campos, tabela, *where
                ...
            
            case "INSERT": # tabela, campos
                ...
            
            case "DELETE": # ??
                ...
            
            case "ALTER": # tabela, alteração estrutural
                ...
                
            case "UPDATE": # tabela, registro, alteração
                ...