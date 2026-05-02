import logging

import grpc
from sqlalchemy import select
from . import customer_service_pb2, customer_service_pb2_grpc
from .database import AsyncSessionLocal
from .models import Person

logger=logging.getLogger(__name__)

class CustomerServicer():
    """gRPC servicer for Customer Service.

    Implements the CustomerService defined in Customer_service.proto
    """

    async def UpdateCustomerStatus(self,
        request:customer_service_pb2.UpdateCustomerStatusRequest,
        context:grpc.aio.ServicerContext)->customer_service_pb2.CustomerResponse:
        """Update Customer status via gRPC.

        This replaces the HTTP PATCH endpoint for inter-service communication
        """
        try:
            logger.info(
                f"gRPC: UpdateCustomerStatus called - "
                f"Customer_id={request.id}, status={request.status}"
            )

            async with AsyncSessionLocal()as db:

                result=await db.execute(
                    select(Person).where(Person.id==request.customer_id)
                )

                customer=result.scalar_one_or_none()
                if not customer:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"Customer {request.customer_id} not found")
                    return customer_service_pb2.CustomerResponse()
                customer.status=request.status
                customer.version+=1

                await db.commit()
                await db.refresh(customer)

                logger.info(
                     f"Customer {customer.id} status updated to '{customer.status}' "
                    f"via gRPC (version {customer.version})"
                )

                return self._customer_to_proto(customer)
            
        except Exception as e:
            logger.info(f"✗ gRPC error in UpdateCustomerStatus: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return customer_service_pb2.CustomerResponse()
        

    