FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright font stubs (Debian bookworm/trixie compatibility)
RUN for pkg in ttf-ubuntu-font-family ttf-unifont; do \
      mkdir -p /tmp/$pkg/DEBIAN \
      && printf "Package: $pkg\nVersion: 1.0\nArchitecture: all\nMaintainer: s <s@s.com>\nDescription: stub\n" \
         > /tmp/$pkg/DEBIAN/control \
      && dpkg-deb --build /tmp/$pkg /tmp/ \
      && dpkg -i /tmp/${pkg}_1.0_all.deb; \
    done \
    && rm -rf /tmp/ttf-ubuntu-font-family /tmp/ttf-unifont /tmp/*.deb

RUN playwright install --with-deps chromium

COPY app/ ./app/

ENV DATA_DIR=/data

COPY run.sh /run.sh
RUN chmod +x /run.sh

EXPOSE 8080

CMD ["/run.sh"]
