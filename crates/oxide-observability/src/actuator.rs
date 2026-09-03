inventory::submit! {
    oxide_web::ManagementRoute {
        path: "/actuator/prometheus",
        endpoint: "prometheus",
        register: |_ctx, router| {
            Ok(router.route(
                "/actuator/prometheus",
                axum::routing::get(|| async {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        crate::metrics::prometheus_handle().render(),
                    )
                }),
            ))
        },
    }
}
