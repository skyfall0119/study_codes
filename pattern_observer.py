from typing import List, Dict
import json
from fastapi import APIRouter,WebSocket, WebSocketDisconnect, Depends
# from service.webSocket_service import VoiceRoomManager

# Subject in the Observer Pattern
class VoiceRoomSubject:
    def __init__(self):
        self.observers: Dict[str, WebSocket] = {}

    async def attach(self, username: str, websocket: WebSocket):
        if username in self.observers:
            print("already in the room")
            old_ws = self.observers[username]
            await old_ws.close()
            self.observers.pop(username)

        
        await websocket.accept()
        self.observers[username] = websocket

        # Notify the new observer with current state
        await websocket.send_text(json.dumps({
            "type": "initial-users",
            "users": list(self.observers.keys())
        }))

        # Notify all other observers about the new one
        await self.notify(json.dumps({
            "type": "join",
            "users": username
        }), exclude=username)
        print(self.observers)
        return self.observers.keys()

    def detach(self, username:str) -> str:
        print(f"removing. remaining {self.observers}")
        if username in self.observers:
            self.observers[username].close()
            return self.observers.pop(username, None)
        return None
        
    async def notify(self, message:str, exclude:str = None):
        for user, ws in self.observers.items():
            if user != exclude:
                await ws.send_text(message)
            

class VoiceRoomManager:
    def __init__(self):
        self.subjects: Dict[str, VoiceRoomSubject] = {}

    def get_subject(self, room_name: str) -> VoiceRoomSubject:
        if room_name not in self.subjects:
            self.subjects[room_name] = VoiceRoomSubject()
        return self.subjects[room_name]
            
       
       
router = APIRouter()
voice_manager = VoiceRoomManager()     
            

@router.websocket("/voice/{room}")
async def voice_websocket_endpoint(websocket: WebSocket, room: str, user: str = Depends(get_user)):
    # get room
    subject = voice_manager.get_subject(room)

    # attach user to room
    parts = await subject.attach(user, websocket)
    print(f"participants!! {parts}")
    try:
        # voicechat
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            message['from'] = user
            await subject.notify(json.dumps(message), exclude=websocket)

    # user disconnected
    except WebSocketDisconnect:
        removed_user = subject.detach(user)
        if removed_user:
            await subject.notify(json.dumps({
                "type": "leave",
                "users": user
            }))