FROM python:3.12-alpine
ENV PYTHONUNBUFFERED=1
# NFS client for direct media access
RUN apk add --no-cache nfs-utils
WORKDIR /app
COPY app.py agent.py ./
EXPOSE 8000
# Default: server only. Use AGENT_MODE=1 to also run agent.
CMD ["sh", "-c", "if [ \"$AGENT_MODE\" = \"1\" ]; then python3 agent.py --server http://localhost:8000 --user ${AGENT_USER:-default} --daemon & fi; python3 app.py"]
