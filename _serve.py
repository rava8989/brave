import http.server, socketserver, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
socketserver.TCPServer(("", 8788), http.server.SimpleHTTPRequestHandler).serve_forever()
