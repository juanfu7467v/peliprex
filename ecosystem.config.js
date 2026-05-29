module.exports = {
  apps : [{
    name: "backend",
    script: "index.js",
    cwd: "/app/backend",
    watch: false,
    exec_mode: "fork",
    env: {
      "PORT": 8080,
      "NODE_ENV": "production"
    }
  }, {
    name: "streaming",
    script: "main.py",
    cwd: "/app/streaming",
    interpreter: "/opt/venv/bin/python3",
    watch: false,
    exec_mode: "fork",
    env: {
      "PORT": 8081,
      "PYTHONUNBUFFERED": "1",
      "PYTHONDONTWRITEBYTECODE": "1",
      "NODE_ENV": "production"
    }
  }]
};
