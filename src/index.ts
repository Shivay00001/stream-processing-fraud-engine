
import dotenv from 'dotenv';
import { KafkaConsumer } from './kafka/consumer';
import { FraudEngine } from './engine/fraud-engine';
import { logger } from './utils/logger';

dotenv.config();

const main = async () => {
    try {
        const fraudEngine = new FraudEngine();
        const kafkaConsumer = new KafkaConsumer(fraudEngine);

        await kafkaConsumer.connect();
        await kafkaConsumer.subscribe('transactions');
        await kafkaConsumer.run();

        logger.info('Fraud Engine started successfully');

        // Graceful shutdown
        const shutdown = async () => {
            await kafkaConsumer.disconnect();
            process.exit(0);
        };

        process.on('SIGTERM', shutdown);
        process.on('SIGINT', shutdown);
    } catch (error) {
        logger.error('Failed to start Fraud Engine', error);
        process.exit(1);
    }
};

main();
