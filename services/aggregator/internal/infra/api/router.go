package api

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NewRouter monta o engine do Gin com middleware de recovery e log
// estruturado via slog (request-level).
//
// gin.New() em vez de gin.Default() porque o Logger default do Gin escreve
// em texto plano, o que conflitaria com o requisito não-funcional de
// "logs estruturados (JSON)". Trocamos por um middleware próprio sobre slog.
//
// gin.SetMode(ReleaseMode) suprime as mensagens de debug do framework —
// queremos só nossos logs JSON no stdout.
func NewRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// otelgin antes do slogMiddleware: assim o context que o slog vê já
	// carrega o span ativo, e o `traceparent` recebido em headers é
	// extraído automaticamente (pronto pra propagação client→server).
	r.Use(otelgin.Middleware("aggregator"), slogMiddleware(), gin.Recovery())

	r.GET("/metrics/:developer_id", h.GetEvents)
	r.GET("/metrics/:developer_id/summary", h.GetSummary)
	r.GET("/health", h.GetHealth)
	return r
}

// slogMiddleware loga cada requisição em JSON via slog default. Substitui
// o gin.Logger pra manter o pipeline de logs unificado.
func slogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.InfoContext(c.Request.Context(), "http request",
			"method", c.Request.Method,
			"path", c.FullPath(),
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}
