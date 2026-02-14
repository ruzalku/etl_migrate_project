from src.extractors.postgresql import Storage
from src.loaders.mongodb import Loader
from src.crud.json_state import JSONStateManager
import asyncio
from src.schema.enums import Mode
import json
from src.core.json_encoder import CustomJSONEncoder

async def main():
    config = {
        "dbname": "testdb",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432
    }
    manager = JSONStateManager()
    extractor = Storage(state_manager=manager, config=config)
    extractor.update_row = 'x2'
    extractor.cdc = True
    loader = Loader(db_name='testdb', config={'host': "mongodb://localhost:27017"})
    
    await extractor.start()
    await loader.start()

    
    print('start complete')
    state = manager.get_state('pg_test')
    
    while True:
    
        data = await extractor.get_objs(index="test", batch_size=2, last_state=state)
        print('data getted:', len(data))
        print(json.dumps(data, cls=CustomJSONEncoder, indent=4))

        if data:
            state = manager.get_state('pg_test')
            await loader.save_objs(index='test', objs=data)
            print(f"Saved {len(data)} docs to MongoDB.test")
        else:
            break
        
        await asyncio.sleep(2)
    
    await extractor.stop()
    await loader.stop()

    
if __name__ == '__main__':
    asyncio.run(main())
