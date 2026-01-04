import os
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests


class ProxyHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler для прокси-сервиса с поддержкой Strangler Fig паттерна"""
    
    def __init__(self, *args, **kwargs):
        self.monolith_url = os.getenv('MONOLITH_URL', 'http://monolith:8080')
        self.movies_service_url = os.getenv('MOVIES_SERVICE_URL', 'http://movies-service:8081')
        self.events_service_url = os.getenv('EVENTS_SERVICE_URL', 'http://events-service:8082')
        self.gradual_migration = os.getenv('GRADUAL_MIGRATION', 'false').lower() == 'true'
        self.movies_migration_percent = int(os.getenv('MOVIES_MIGRATION_PERCENT', '0'))
        super().__init__(*args, **kwargs)
    
    def log_message(self, format, *args):
        print(f"[Proxy] {format % args}")
    
    def do_GET(self):
        self._handle_request()
    
    def do_POST(self):
        self._handle_request()
    
    def do_PUT(self):
        self._handle_request()
    
    def do_DELETE(self):
        self._handle_request()
    
    def _handle_request(self):
        path = self.path

        if path == '/health':
            self._send_response(200, 'Strangler Fig Proxy is healthy', content_type='text/plain')
            return

        target_url = self._determine_target(path)
        
        if not target_url:
            self._send_error_response(404, 'Not Found')
            return

        self._proxy_request(target_url)
    
    def _determine_target(self, path):
        if path == '/api/movies/health':
            return f"{self.movies_service_url}{path}"

        if path.startswith('/api/movies'):
            if self.gradual_migration:
                random_percent = random.randint(0, 99)
                if random_percent < self.movies_migration_percent:
                    self.log_message(f"Routing to movies-service (random: {random_percent} < {self.movies_migration_percent})")
                    return f"{self.movies_service_url}{path}"
                else:
                    self.log_message(f"Routing to monolith (random: {random_percent} >= {self.movies_migration_percent})")
                    return f"{self.monolith_url}{path}"
            else:
                return f"{self.monolith_url}{path}"

        if path.startswith('/api/events'):
            return f"{self.events_service_url}{path}"

        return f"{self.monolith_url}{path}"
    
    def _proxy_request(self, target_url):
        try:
            content_length = self.headers.get('Content-Length', 0)
            body = None
            if content_length:
                body = self.rfile.read(int(content_length))

            headers = {}
            for header, value in self.headers.items():
                if header.lower() not in ['host', 'connection', 'content-length']:
                    headers[header] = value

            method = self.command

            response = requests.request(
                method=method,
                url=target_url,
                headers=headers,
                data=body,
                timeout=30
            )

            self._send_response(
                status_code=response.status_code,
                content=response.content,
                headers=dict(response.headers)
            )
            
        except requests.exceptions.RequestException as e:
            self.log_message(f"Error proxying request to {target_url}: {str(e)}")
            self._send_error_response(502, f'Bad Gateway: {str(e)}')
        except Exception as e:
            self.log_message(f"Unexpected error: {str(e)}")
            self._send_error_response(500, 'Internal Server Error')
    
    def _send_response(self, status_code, content, headers=None, content_type=None):
        self.send_response(status_code)

        if content_type:
            self.send_header('Content-Type', content_type)
        elif headers and 'Content-Type' in headers:
            self.send_header('Content-Type', headers['Content-Type'])
        else:
            if isinstance(content, str):
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
            else:
                self.send_header('Content-Type', 'application/json')

        if headers:
            for header, value in headers.items():
                if header.lower() not in ['content-type', 'content-length', 'transfer-encoding']:
                    self.send_header(header, value)

        if isinstance(content, str):
            content = content.encode('utf-8')
        
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)
    
    def _send_error_response(self, status_code, message):
        error_body = f'{{"error": "{message}"}}'
        self._send_response(status_code, error_body, content_type='application/json')


def main():
    port = int(os.getenv('PORT', '8000'))

    def handler(*args, **kwargs):
        return ProxyHandler(*args, **kwargs)
    
    server = HTTPServer(('0.0.0.0', port), handler)
    print(f'Starting Strangler Fig Proxy on port {port}')
    print(f'Monolith URL: {os.getenv("MONOLITH_URL", "http://monolith:8080")}')
    print(f'Movies Service URL: {os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")}')
    print(f'Events Service URL: {os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")}')
    print(f'Gradual Migration: {os.getenv("GRADUAL_MIGRATION", "false")}')
    print(f'Movies Migration Percent: {os.getenv("MOVIES_MIGRATION_PERCENT", "0")}%')
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down proxy server...')
        server.shutdown()


if __name__ == '__main__':
    main()

