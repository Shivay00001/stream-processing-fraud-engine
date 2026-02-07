
import Redis from 'ioredis';
import { logger } from '../utils/logger';

interface Transaction {
    id: string;
    userId: string;
    amount: number;
    timestamp: number;
    merchantId: string;
}

export class FraudEngine {
    private redis: Redis;

    constructor() {
        this.redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
    }

    public async processTransaction(tx: Transaction) {
        logger.info(`Processing transaction ${tx.id} for user ${tx.userId}`);

        const isFraud = await this.checkRules(tx);

        if (isFraud) {
            logger.warn(`FRAUD DETECTED: Transaction ${tx.id} for user ${tx.userId}`);
            // In a real system, publish to 'fraud-alerts' topic
        } else {
            logger.info(`Transaction ${tx.id} approved`);
        }

        // Update state
        await this.updateState(tx);
    }

    private async checkRules(tx: Transaction): Promise<boolean> {
        // Rule 1: Amount > 10000 -> Flag
        if (tx.amount > 10000) return true;

        // Rule 2: High velocity (more than 5 tx in 1 minute)
        const recentTxCount = await this.redis.get(`tx_count:${tx.userId}`);
        if (recentTxCount && parseInt(recentTxCount) > 5) return true;

        return false;
    }

    private async updateState(tx: Transaction) {
        const key = `tx_count:${tx.userId}`;
        await this.redis.incr(key);
        await this.redis.expire(key, 60); // 1 minute window
    }
}
