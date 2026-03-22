package com.kafka.exam.kafkaexam.consumer.dlt

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/dlt/dashboard")
class DltDashboardController(
    private val dashboardService: DltDashboardService,
    private val analysisService: DltAnalysisService
) {

    /**
     * 대시보드 요약
     * GET /api/dlt/dashboard/summary
     */
    @GetMapping("/summary")
    fun getSummary(): ResponseEntity<DashboardSummary> {
        return ResponseEntity.ok(dashboardService.getDashboardSummary())
    }

    /**
     * 실시간 현황 (경량 폴링용)
     * GET /api/dlt/dashboard/realtime
     */
    @GetMapping("/realtime")
    fun getRealTimeStatus(): ResponseEntity<RealTimeStatus> {
        return ResponseEntity.ok(dashboardService.getRealTimeStatus())
    }

    /**
     * 상세 대시보드
     * GET /api/dlt/dashboard/detailed
     */
    @GetMapping("/detailed")
    fun getDetailedDashboard(): ResponseEntity<DetailedDashboard> {
        return ResponseEntity.ok(dashboardService.getDetailedDashboard())
    }

    /**
     * 헬스 체크
     * GET /api/dlt/dashboard/health
     */
    @GetMapping("/health")
    fun getHealthStatus(): ResponseEntity<DltHealthStatus> {
        return ResponseEntity.ok(dashboardService.getHealthStatus())
    }

    /**
     * 토픽별 상세 현황
     * GET /api/dlt/dashboard/topics/{topic}
     */
    @GetMapping("/topics/{topic}")
    fun getTopicDetail(@PathVariable topic: String): ResponseEntity<TopicDetail> {
        return ResponseEntity.ok(dashboardService.getTopicDetail(topic))
    }

    /**
     * 에러 패턴 분석
     * GET /api/dlt/dashboard/analysis/errors
     */
    @GetMapping("/analysis/errors")
    fun getErrorPatterns(): ResponseEntity<ErrorPatternAnalysis> {
        return ResponseEntity.ok(analysisService.analyzeErrorPatterns())
    }

    /**
     * 토픽별 분석
     * GET /api/dlt/dashboard/analysis/topics
     */
    @GetMapping("/analysis/topics")
    fun getTopicAnalysis(): ResponseEntity<List<TopicFailureAnalysis>> {
        return ResponseEntity.ok(analysisService.analyzeByTopic())
    }

    /**
     * 시간대별 트렌드
     * GET /api/dlt/dashboard/analysis/trends?hours=24
     */
    @GetMapping("/analysis/trends")
    fun getHourlyTrend(
        @RequestParam(defaultValue = "24") hours: Int
    ): ResponseEntity<List<HourlyTrend>> {
        return ResponseEntity.ok(analysisService.analyzeHourlyTrend(hours))
    }

    /**
     * 재시도 효율성 분석
     * GET /api/dlt/dashboard/analysis/retry-efficiency
     */
    @GetMapping("/analysis/retry-efficiency")
    fun getRetryEfficiency(): ResponseEntity<RetryEfficiencyAnalysis> {
        return ResponseEntity.ok(analysisService.analyzeRetryEfficiency())
    }
}
