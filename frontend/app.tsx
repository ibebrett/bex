import React from "react";
import ReactDOM from "react-dom";

class API {
  websocket: WebSocket;

  constructor() {
    this.websocket = new WebSocket("http://localhost:8765");
    this.websocket.onmessage = (event: MessageEvent) => {
      // Process a message
    };
  }
}
const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<h1>Hello, blahworld!</h1>);
