
import { Kafka, Consumer } from 'kafkajs';
import { FraudEngine } from '../engine/fraud-engine';
import { logger } from '../utils/logger';

export class KafkaConsumer {
    private kafka: Kafka;
    private consumer: Consumer;
    private engine: FraudEngine;

    constructor(engine: FraudEngine) {
        this.engine = engine;
        this.kafka = new Kafka({
            clientId: 'fraud-engine',
            brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
        });

        this.consumer = this.kafka.consumer({ groupId: 'fraud-engine-group' });
    }

    public async connect() {
        await this.consumer.connect();
        logger.info('Connected to Kafka');
    }

    public async subscribe(topic: string) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
    }

    public async run() {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (!message.value) return;
                try {
                    const transaction = JSON.parse(message.value.toString());
                    await this.engine.processTransaction(transaction);
                } catch (e) {
                    logger.error('Error processing message', e);
                }
            },
        });
    }

    public async disconnect() {
        await this.consumer.disconnect();
    }
}
