const traceUtils = require('./tracing')('server', 'voting-app-server');
const Pyroscope = require('@pyroscope/nodejs');
const { expressMiddleware } = require('@pyroscope/nodejs');
const logUtils = require('./logging')('voting-app-server', 'server');
const cors = require('cors');

(async () => {
    const traceObj = await traceUtils();
    const logEntry = await logUtils(traceObj);
    const { tracer, api } = traceObj;

    const promClient = require('prom-client');
    const express = require('express');
    const bodyParser = require('body-parser');
    const { Client } = require('pg');
    const { nameSet, servicePrefix, spanTag } = require('./endpoints')();

    const app = express();
    const register = promClient.register;
    register.setContentType(promClient.Registry.OPENMETRICS_CONTENT_TYPE);

    const teardownTimeout = 24 * 60 * 60 * 1000;
    let teardownInProgress = false;

    app.use(bodyParser.json());
    app.use(cors());

    let pgClient;

    const responseBucket = new promClient.Histogram({
        name: 'voting_request_times',
        help: 'Response times for the endpoints',
        labelNames: [
            'method',
            'status',
            spanTag,
            'endpoint',
            'table',
            'rows',
            'columns'
        ],
        buckets: [10, 20, 50, 100, 200, 500, 1000, 2000, 4000, 8000, 16000],
        enableExemplars: true,
    });

    const responseMetric = (details) => {
        const timeMs = Date.now() - details.start;
        const spanContext = api.trace.getSpan(
            api.context.active()
        ).spanContext();

        responseBucket.observe({
            labels: details.labels,
            value: timeMs,
            exemplarLabels: {
                traceID: spanContext.traceId,
                spanID: spanContext.spanId,
            },
        });
    };

    // --------------------------------------------------
    // Prometheus metrics endpoint
    // --------------------------------------------------

    app.get('/metrics', async (req, res) => {
        res.set('Content-Type', register.contentType);
        res.send(await register.metrics());
    });

    // --------------------------------------------------
    // Health check endpoint
    // --------------------------------------------------

    app.get('/api/health', (req, res) => {
        const host = req.get('host');
        const forwardedHost = req.get('x-forwarded-host');
        const protocol = req.get('x-forwarded-proto') || 'http';

        // Same logic as topic creation for URL generation
        let votingHost = forwardedHost || host;

        if (votingHost) {
            if (protocol === 'http' && votingHost.endsWith(':80')) {
                votingHost = votingHost.replace(':80', '');
            } else if (protocol === 'https' && votingHost.endsWith(':443')) {
                votingHost = votingHost.replace(':443', '');
            }

            if (votingHost.includes(':5000')) {
                votingHost = votingHost.replace(':5000', '');
            }
        }

        res.json({
            status: 'healthy',
            timestamp: new Date().toISOString(),
            headers: {
                host: req.get('host'),
                'x-forwarded-host': req.get('x-forwarded-host'),
                'x-forwarded-proto': req.get('x-forwarded-proto'),
                'user-agent': req.get('user-agent')
            },
            urlGeneration: {
                originalHost: host,
                forwardedHost: forwardedHost,
                finalVotingHost: votingHost,
                protocol: protocol,
                sampleVotingUrl: `${protocol}://${votingHost}/vote/123`
            }
        });
    });

    // --------------------------------------------------
    // Pyroscope
    // --------------------------------------------------

    Pyroscope.init({
        appName: 'voting-app-database-server'
    });

    app.use(expressMiddleware());

    // --------------------------------------------------
    // Create Topic
    // --------------------------------------------------

    app.post('/api/topics', async (req, res) => {
        const currentSpan = api.trace.getSpan(api.context.active());
        const traceId = currentSpan.spanContext().traceId;

        let metricBody = {
            labels: {
                method: 'POST',
                endpoint: 'createTopic'
            },
            start: Date.now(),
        };

        const { topic, description } = req.body;

        // Validate input data
        if (!topic || !description) {
            metricBody.labels.status = '400';
            responseMetric(metricBody);

            res.status(400).send(
                'Topic name and description are required.'
            );

            return;
        }

        try {
            // Insert topic into the database
            const query = `
                INSERT INTO topics (name, description)
                VALUES ($1, $2)
                RETURNING id
            `;

            const result = await pgClient.query(query, [
                topic,
                description
            ]);

            // Get total number of topics
            const tableDetail = await pgClient.query(
                `SELECT COUNT(*) AS row_count FROM topics`
            );

            // Metrics after successful topic creation
            metricBody.labels.status = '201';
            metricBody.labels.table = 'topics';
            metricBody.labels.rows = tableDetail.rows[0].row_count;
            metricBody.labels.columns = 'name, description';

            responseMetric(metricBody);

            // Log creation
            logEntry({
                level: 'info',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'createTopic',
                message: `Topic created successfully with ID ${result.rows[0].id}`,
                table: 'topics',
                rows: tableDetail.rows[0].row_count,
                columns: 'name, description',
            });

            // Generate voting URL
            const host = req.get('host');
            const forwardedHost = req.get('x-forwarded-host');
            const protocol = req.get('x-forwarded-proto') || 'http';

            let votingHost = forwardedHost || host;

            // Remove standard ports
            if (votingHost) {
                if (
                    protocol === 'http' &&
                    votingHost.endsWith(':80')
                ) {
                    votingHost = votingHost.replace(':80', '');
                } else if (
                    protocol === 'https' &&
                    votingHost.endsWith(':443')
                ) {
                    votingHost = votingHost.replace(':443', '');
                }

                // Remove internal backend port
                if (votingHost.includes(':5000')) {
                    votingHost = votingHost.replace(':5000', '');
                }
            }

            const votingUrl =
                `${protocol}://${votingHost}/vote/${result.rows[0].id}`;

            console.log('Generated voting URL:', votingUrl);
            console.log('Host header:', req.get('host'));
            console.log(
                'X-Forwarded-Host header:',
                req.get('x-forwarded-host')
            );
            console.log('Final voting host:', votingHost);
            console.log('Protocol:', protocol);

            res.status(201).json({
                message: 'Topic created successfully!',
                votingUrl
            });

        } catch (err) {
            metricBody.labels.status = '500';
            responseMetric(metricBody);

            logEntry({
                level: 'error',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'createTopic',
                message: `Error creating topic: ${err.message}`,
                table: 'topics',
                rows: 0,
                columns: 'name, description',
            });

            res.status(500).send('Error creating topic.');
        }
    });

    // --------------------------------------------------
    // Get Topic Description
    // --------------------------------------------------

    app.get('/api/topics/:topic/vote', async (req, res) => {
        const currentSpan = api.trace.getSpan(api.context.active());
        const traceId = currentSpan.spanContext().traceId;

        let metricBody = {
            labels: {
                method: 'GET',
                endpoint: 'get-topic-description'
            },
            start: Date.now(),
        };

        const { topic } = req.params;

        try {
            const result = await pgClient.query(
                `SELECT name, description FROM topics WHERE id = $1`,
                [topic]
            );

            const description = result.rows[0]?.description;
            const topicName = result.rows[0]?.name;

            if (!description) {
                metricBody.labels.status = '404';
                responseMetric(metricBody);

                logEntry({
                    level: 'info',
                    traceID: traceId,
                    namespace: process.env.NAMESPACE,
                    job: `${servicePrefix}-server`,
                    endpoint: 'get-topic-description',
                    message: `Topic '${topic}' not found`,
                    table: 'topics',
                    rows: 0,
                    columns: 'description',
                });

                return res.status(404).send('Topic not found');
            }

            metricBody.labels.status = '200';
            responseMetric(metricBody);

            logEntry({
                level: 'info',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'get-topic-description',
                message: `Fetched topic description for '${topic}'`,
                table: 'topics',
                rows: 1,
                columns: 'description',
            });

            res.json({
                topic: topicName,
                description
            });

        } catch (err) {
            metricBody.labels.status = '500';
            responseMetric(metricBody);

            logEntry({
                level: 'error',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'get-topic-description',
                message:
                    `Error fetching topic description for '${topic}': ${err.message}`,
                table: 'topics',
                rows: 0,
                columns: 'description',
            });

            res.status(500).json({
                error: 'Internal Server Error'
            });
        }
    });

    // --------------------------------------------------
    // Submit Vote
    // --------------------------------------------------

    app.post('/api/topics/:topic/vote', async (req, res) => {
        const currentSpan = api.trace.getSpan(api.context.active());
        const traceId = currentSpan.spanContext().traceId;

        let metricBody = {
            labels: {
                method: 'POST',
                endpoint: 'vote'
            },
            start: Date.now(),
        };

        const { topic } = req.params;
        const { vote, name } = req.body;

        // Validate input
        if (!vote || !name) {
            metricBody.labels.status = '400';
            responseMetric(metricBody);

            logEntry({
                level: 'info',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'vote',
                message: 'Vote and name are required',
                table: 'votes',
                rows: 0,
                columns: 'vote, name',
            });

            return res.status(400).json({
                error: 'Vote and name are required'
            });
        }

        try {
            const query = `
                INSERT INTO votes (topic_id, name, vote)
                VALUES ($1, $2, $3)
                RETURNING id
            `;

            const result = await pgClient.query(query, [
                topic,
                name,
                vote
            ]);

            // Get total vote count
            const tableDetail = await pgClient.query(
                `SELECT COUNT(*) AS row_count
                 FROM votes
                 WHERE topic_id = $1`,
                [topic]
            );

            metricBody.labels.status = '201';
            metricBody.labels.table = 'votes';
            metricBody.labels.rows = tableDetail.rows[0].row_count;
            metricBody.labels.columns = 'topic_id, name, vote';

            responseMetric(metricBody);

            logEntry({
                level: 'info',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'vote',
                message:
                    `Vote submitted successfully for topic '${topic}' by ${name} with vote ID ${result.rows[0].id}`,
                table: 'votes',
                rows: tableDetail.rows[0].row_count,
                columns: 'topic_id, name, vote',
            });

            res.status(201).json({
                message: 'Vote counted!'
            });

        } catch (err) {
            metricBody.labels.status = '500';
            responseMetric(metricBody);

            logEntry({
                level: 'error',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: 'vote',
                message:
                    `Error processing vote for topic '${topic}': ${err.message}`,
                table: 'votes',
                rows: 0,
                columns: 'topic_id, name, vote',
            });

            res.status(500).json({
                error: 'Internal Server Error'
            });
        }
    });

    // --------------------------------------------------
    // Get Voting Results
    // --------------------------------------------------

    app.get('/api/topics/:topic/results', async (req, res) => {
        const currentSpan = api.trace.getSpan(api.context.active());
        const traceId = currentSpan.spanContext().traceId;

        let metricBody = {
            labels: {
                method: 'GET',
                endpoint: '/api/topics/:topic/results'
            },
            start: Date.now(),
        };

        const { topic } = req.params;

        if (!topic) {
            metricBody.labels.status = '400';
            responseMetric(metricBody);

            res.status(400).send('Topic ID is required.');

            return;
        }

        try {
            const result = await pgClient.query(
                `SELECT vote, name
                 FROM votes
                 WHERE topic_id = $1`,
                [topic]
            );

            const topicName = await pgClient.query(
                `SELECT name
                 FROM topics
                 WHERE id = $1`,
                [topic]
            );

            const agreeVotes = [];
            const notAgreeVotes = [];

            result.rows.forEach((row) => {
                if (row.vote === 'agree') {
                    agreeVotes.push(row.name);
                } else if (row.vote === 'not_agree') {
                    notAgreeVotes.push(row.name);
                }
            });

            const tableDetail = await pgClient.query(
                `SELECT COUNT(*) AS row_count
                 FROM votes
                 WHERE topic_id = $1`,
                [topic]
            );

            metricBody.labels.status = '200';
            metricBody.labels.table = 'votes';
            metricBody.labels.rows = tableDetail.rows[0].row_count;
            metricBody.labels.columns = 'vote';

            responseMetric(metricBody);

            logEntry({
                level: 'info',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: '/api/topics/:topic/results',
                message: 'Result fetched successfully',
                table: 'votes',
                rows: tableDetail.rows[0].row_count,
                columns: 'vote',
            });

            res.status(200).json({
                topic: topicName.rows[0].name,
                countAgree: agreeVotes.length,
                countNotAgree: notAgreeVotes.length,
                votes: {
                    agree: agreeVotes,
                    notAgree: notAgreeVotes,
                },
            });

        } catch (err) {
            metricBody.labels.status = '500';
            responseMetric(metricBody);

            logEntry({
                level: 'error',
                traceID: traceId,
                namespace: process.env.NAMESPACE,
                job: `${servicePrefix}-server`,
                endpoint: '/api/topics/:topic/results',
                message: `Error fetching result: ${err.message}`,
                table: 'votes',
                rows: 0,
                columns: 'vote',
            });

            res.status(500).send('Error fetching result.');
        }
    });

    // --------------------------------------------------
    // PostgreSQL Connection
    // --------------------------------------------------

    const startServer = async () => {
        const requestSpan = tracer.startSpan('server');

        await api.context.with(
            api.trace.setSpan(api.context.active(), requestSpan),
            async () => {
                try {
                    logEntry({
                        level: 'info',
                        job: `${servicePrefix}-server`,
                        namespace: process.env.NAMESPACE,
                        message: 'Connecting to Postgres...',
                    });

                    // Read database configuration from environment variables.
                    // This allows the same Docker image to work in
                    // DEV, TEST and PROD.
                    const dbHost = process.env.DB_HOST;
                    const dbPort = Number(process.env.DB_PORT || 5432);
                    const dbUser = process.env.DB_USER;
                    const dbPassword = process.env.DB_PASSWORD;
                    const dbName = process.env.DB_NAME || spanTag;

                    // Validate required database configuration.
                    if (!dbHost || !dbUser || !dbPassword) {
                        throw new Error(
                            'Missing required database environment variables: DB_HOST, DB_USER, DB_PASSWORD'
                        );
                    }

                    // Connect directly to the configured database.
                    pgClient = new Client({
                        host: dbHost,
                        port: dbPort,
                        user: dbUser,
                        password: dbPassword,
                        database: dbName,
                    });

                    await pgClient.connect();

                    logEntry({
                        level: 'info',
                        job: `${servicePrefix}-server`,
                        namespace: process.env.NAMESPACE,
                        message:
                            `Connected to PostgreSQL database '${dbName}'`,
                    });

                    // --------------------------------------------------
                    // Create votes table if it doesn't exist
                    // --------------------------------------------------

                    await pgClient.query(`
                        CREATE TABLE IF NOT EXISTS votes (
                            id SERIAL PRIMARY KEY,
                            topic_id INT NOT NULL,
                            name VARCHAR(255) NOT NULL,
                            vote VARCHAR(255) NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    `);

                    // --------------------------------------------------
                    // Create topics table if it doesn't exist
                    // --------------------------------------------------

                    await pgClient.query(`
                        CREATE TABLE IF NOT EXISTS topics (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    `);

                    logEntry({
                        level: 'info',
                        namespace: process.env.NAMESPACE,
                        job: `${servicePrefix}-server`,
                        message: 'Tables ensured in database.',
                    });

                } catch (err) {
                    if (pgClient) {
                        await pgClient.end();
                    }

                    logEntry({
                        level: 'error',
                        namespace: process.env.NAMESPACE,
                        job: `${servicePrefix}-server`,
                        message: `Error starting database: ${err}`,
                    });

                    // Retry database connection after 5 seconds.
                    setTimeout(startServer, 5000);

                } finally {
                    requestSpan.end();
                }
            }
        );
    };

    // --------------------------------------------------
    // Start HTTP Server
    // --------------------------------------------------

    const serverPort = Number(process.env.PORT || 5000);

    app.listen(serverPort, '0.0.0.0', () =>
        logEntry({
            level: 'info',
            namespace: process.env.NAMESPACE,
            job: `${servicePrefix}-server`,
            message:
                `${servicePrefix} server is running on port ${serverPort}`,
        })
    );

    startServer();

})();