
module.exports = {
  apps : [{
    name: "backend",
    script: "backend/index.js",
    watch: false,
    env: {
      "PORT": 8080,
      "NODE_ENV": "production",
    }
  }, {
    name: "streaming",
    script: "streaming/main.py",
    interpreter: "python3",
    watch: false,
    env: {
      "STREAMING_PORT": 8081,
      "PYTHONUNBUFFERED": 1,
      "PYTHONDONTWRITEBYTECODE": 1,
      "NODE_ENV": "production",
    }
  }]
};
